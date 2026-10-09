import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

/// A remote whose pushes wait for [gate] and whose pulls are counted.
class _SlowRemote extends RemoteAdapter {
  Completer<void> gate = Completer<void>()..complete();
  int pushes = 0;
  int pulls = 0;
  bool pushing = false;
  bool pulledWhilePushing = false;

  @override
  Future<PushOutcome> push(PushRequest request) async {
    pushing = true;
    await gate.future;
    pushing = false;
    pushes++;
    return PushOutcome.success(serverId: '${100 + pushes}');
  }

  @override
  Future<PullPage> pull(PullRequest request) async {
    if (pushing) pulledWhilePushing = true;
    pulls++;
    return const PullPage(records: []);
  }

  void hold() => gate = Completer<void>();
  void release() => gate.complete();
}

class _CountingConnectivity implements ConnectivitySource {
  int listeners = 0;
  late final StreamController<bool> _controller =
      StreamController<bool>.broadcast(
    onListen: () => listeners++,
    onCancel: () => listeners--,
  );

  @override
  bool get isOnline => true;

  @override
  Stream<bool> get onChanged => _controller.stream;

  Future<void> dispose() => _controller.close();
}

class _FlakyStore extends InMemorySyncStore {
  int failures;
  _FlakyStore(this.failures);

  @override
  Future<void> init() async {
    if (failures > 0) {
      failures--;
      throw StateError('database is locked');
    }
  }
}

class _SlowInitStore extends InMemorySyncStore {
  @override
  Future<void> init() => Future<void>.delayed(const Duration(milliseconds: 30));
}

void main() {
  late _SlowRemote remote;
  late FakeLocalDb db;

  setUp(() {
    remote = _SlowRemote();
    db = FakeLocalDb();
  });

  SyncEngine engine({
    SyncConfig config = manualConfig,
    SyncStore? store,
    ConnectivitySource? connectivity,
    LocalAdapter? local,
  }) {
    final sync = SyncEngine(
      store: store ?? InMemorySyncStore(),
      connectivity: connectivity,
      config: config,
      entities: [
        SyncEntityConfig(
          name: 'notes',
          remote: remote,
          local: local ?? db.adapter('notes'),
        ),
      ],
    );
    addTearDown(sync.dispose);
    return sync;
  }

  group('one run at a time', () {
    test('a sync requested during a push-only run still pulls', () async {
      final sync = engine();
      await sync.recordCreate('notes', 'a', {});
      remote.hold();
      final pushOnly = sync.pushNow();
      await settle(const Duration(milliseconds: 10));
      final full = sync.syncNow();
      remote.release();

      expect((await pushOnly).pushed, 1);
      final followUp = await full;
      expect(followUp.completed, isTrue);
      expect(remote.pulls, 1);
    });

    test('pullNow waits for the push in progress', () async {
      final sync = engine();
      await sync.recordCreate('notes', 'a', {});
      remote.hold();
      final push = sync.pushNow();
      await settle(const Duration(milliseconds: 10));
      final pull = sync.pullNow();
      await settle(const Duration(milliseconds: 10));
      expect(remote.pulls, 0);
      remote.release();
      await push;
      await pull;
      expect(remote.pulls, 1);
      expect(remote.pulledWhilePushing, isFalse);
    });

    test('requests made during a run merge into one follow-up', () async {
      final sync = engine();
      final runs = <SyncEvent>[];
      sync.events
          .where((e) => e.type == SyncEventType.runStarted)
          .listen(runs.add);
      await sync.recordCreate('notes', 'a', {});
      remote.hold();
      final first = sync.pushNow();
      await settle(const Duration(milliseconds: 10));
      await sync.recordCreate('notes', 'b', {});
      final followUps = [sync.pushNow(), sync.syncNow(), sync.pullNow()];
      remote.release();
      // The run in progress also picks up 'b' in its next push pass.
      expect((await first).pushed, 2);
      final results = await Future.wait(followUps);

      expect(runs, hasLength(2));
      expect(results.toSet(), hasLength(1)); // one shared follow-up run
      expect(remote.pushes, 2);
      expect(remote.pulls, 1);
    });
  });

  group('triggers', () {
    const auto = SyncConfig(
      syncOnStart: false,
      syncOnResume: false,
      periodicInterval: null,
      debounce: Duration(milliseconds: 20),
    );

    test('a burst of writes is pushed by one debounced run', () async {
      final sync = engine(config: auto);
      final runs = <SyncEvent>[];
      sync.events
          .where((e) => e.type == SyncEventType.runStarted)
          .listen(runs.add);
      await sync.start();
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      await settle(const Duration(milliseconds: 150));
      expect(runs, hasLength(1));
      expect(remote.pushes, 5);
      expect(remote.pulls, 0); // writes only push
    });

    test('a write made during a run is pushed right after it', () async {
      final sync = engine(config: auto);
      await sync.start();
      remote.hold();
      await sync.recordCreate('notes', 'a', {});
      await settle(const Duration(milliseconds: 60)); // run 1 is in flight
      await sync.recordCreate('notes', 'b', {});
      await settle(const Duration(milliseconds: 60));
      remote.release();
      await settle(const Duration(milliseconds: 150));
      expect(remote.pushes, 2);
      expect(sync.status.value.isUpToDate, isTrue);
    });

    test('start syncs at once; pullOnSync: false never pulls', () async {
      final sync = engine(
          config: const SyncConfig(
        syncOnResume: false,
        periodicInterval: null,
        pullOnSync: false,
      ));
      await sync.recordCreate('notes', 'a', {});
      await sync.start();
      await settle(const Duration(milliseconds: 50));
      expect(remote.pushes, 1);
      expect(remote.pulls, 0);
    });

    test('offline writes wait; reconnecting syncs them', () async {
      final connectivity = ManualConnectivity(false);
      addTearDown(connectivity.dispose);
      final sync = engine(config: auto, connectivity: connectivity);
      await sync.start();
      await sync.recordCreate('notes', 'a', {});
      await settle(const Duration(milliseconds: 80));
      expect(remote.pushes, 0);
      expect(sync.status.value.phase, SyncPhase.offline);
      expect(sync.status.value.isOnline, isFalse);

      connectivity.online = true;
      await settle(const Duration(milliseconds: 120));
      expect(remote.pushes, 1);
      expect(remote.pulls, 1); // reconnecting also pulls
      expect(sync.status.value.phase, SyncPhase.idle);
    });

    test('the periodic timer syncs and pulls', () async {
      final sync = engine(
          config: const SyncConfig(
        syncOnStart: false,
        syncOnResume: false,
        periodicInterval: Duration(milliseconds: 50),
        debounce: Duration(milliseconds: 1),
      ));
      await sync.start();
      await settle(const Duration(milliseconds: 180));
      expect(remote.pulls, greaterThanOrEqualTo(2));
      await sync.dispose();
      final pulls = remote.pulls;
      await settle(const Duration(milliseconds: 120));
      expect(remote.pulls, pulls); // stopped by dispose
    });

    test('returning to the foreground syncs and pulls', () async {
      final sync = engine(
          config: const SyncConfig(
        syncOnStart: false,
        syncOnResume: false,
        periodicInterval: null,
        debounce: Duration(milliseconds: 1),
      ));
      await sync.start();
      sync.didChangeAppLifecycleState(AppLifecycleState.paused);
      sync.didChangeAppLifecycleState(AppLifecycleState.resumed);
      await settle(const Duration(milliseconds: 50));
      expect(remote.pulls, 1);
    });

    test('a run can be limited to some entities', () async {
      final other = _SlowRemote();
      final sync = SyncEngine(
        store: InMemorySyncStore(),
        config: manualConfig,
        entities: [
          SyncEntityConfig(
              name: 'notes', remote: remote, local: db.adapter('notes')),
          SyncEntityConfig(
              name: 'other', remote: other, local: db.adapter('other')),
        ],
      );
      addTearDown(sync.dispose);
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('other', 'b', {});
      final result = await sync.syncNow(entities: {'notes'});
      expect(result.pushed, 1);
      expect(result.waiting, 0);
      expect((remote.pushes, remote.pulls), (1, 1));
      expect((other.pushes, other.pulls), (0, 0));
      expect((await sync.pushNow(entities: {'other'})).pushed, 1);
    });

    test('canSync pauses sync without touching the outbox', () async {
      var signedIn = false;
      final sync = engine(
          config: SyncConfig(
        syncOnStart: false,
        syncOnResume: false,
        autoPushAfterWrite: false,
        periodicInterval: null,
        canSync: () async => signedIn,
      ));
      await sync.recordCreate('notes', 'a', {});
      final paused = await sync.syncNow();
      expect(paused.abortedBy, SyncAbortReason.paused);
      expect(sync.status.value.phase, SyncPhase.paused);
      expect(sync.status.value.lastSyncedAt, isNull);
      expect(remote.pushes, 0);

      signedIn = true;
      expect((await sync.syncNow()).pushed, 1);
      expect(sync.status.value.lastSyncedAt, isNotNull);
    });

    test('a backoff left by an earlier session is retried after start',
        () async {
      final store = InMemorySyncStore();
      await store.init();
      final now = DateTime.now();
      await store.insertOperation(SyncOperation(
        id: 'op',
        entity: 'notes',
        localId: 'a',
        type: SyncOpType.create,
        payload: const {},
        attempts: 1,
        createdAt: now,
        updatedAt: now,
        nextAttemptAt: now.add(const Duration(milliseconds: 100)),
      ));
      final sync = engine(config: auto, store: store);
      await sync.start();
      expect(sync.status.value.nextRetryAt, isNotNull);
      await settle(const Duration(milliseconds: 300));
      expect(remote.pushes, 1);
      expect(sync.status.value.nextRetryAt, isNull);
    });
  });

  group('lifecycle', () {
    test('a store that fails to open can be retried', () async {
      final sync = engine(store: _FlakyStore(2));
      await expectLater(sync.start(), throwsStateError);
      // A run reports the error instead of throwing.
      final result = await sync.syncNow();
      expect(result.errors.single, contains('database is locked'));
      expect(sync.status.value.lastError, contains('database is locked'));
      // Third time lucky.
      await sync.start();
      await sync.recordCreate('notes', 'a', {});
      expect((await sync.syncNow()).pushed, 1);
    });

    test('a failing store never raises an unhandled error', () async {
      final uncaught = <Object>[];
      await runZonedGuarded(() async {
        final sync = SyncEngine(
          store: _FlakyStore(100),
          config: manualConfig,
          entities: [
            SyncEntityConfig(
                name: 'notes', remote: remote, local: db.adapter('notes')),
          ],
        );
        await sync.syncNow();
        await sync.pullNow();
        await settle(const Duration(milliseconds: 20));
        await sync.dispose();
      }, (error, stack) => uncaught.add(error));
      expect(uncaught, isEmpty);
    });

    test('dispose during start leaves nothing running', () async {
      final connectivity = _CountingConnectivity();
      addTearDown(connectivity.dispose);
      final sync = engine(
          store: _SlowInitStore(),
          connectivity: connectivity,
          config: const SyncConfig(syncOnResume: false));
      final started = sync.start();
      await sync.dispose();
      await started;
      expect(connectivity.listeners, 0);
      expect(remote.pushes + remote.pulls, 0);
    });

    test('dispose lets the run finish and drops queued work', () async {
      final sync = engine();
      await sync.recordCreate('notes', 'a', {});
      remote.hold();
      final running = sync.pushNow();
      await settle(const Duration(milliseconds: 10));
      final queued = sync.syncNow();
      final disposed = sync.dispose();
      remote.release();
      await disposed;
      expect((await running).pushed, 1);
      expect((await queued).abortedBy, SyncAbortReason.paused);
      expect((await sync.syncNow()).abortedBy, SyncAbortReason.paused);
    });

    test('clear waits for the run, then wipes everything', () async {
      final sync = engine();
      final events = <SyncEventType>[];
      sync.events.listen((e) => events.add(e.type));
      await sync.recordCreate('notes', 'a', {});
      remote.hold();
      final run = sync.syncNow();
      await settle(const Duration(milliseconds: 10));
      final cleared = sync.clear(); // e.g. logout
      remote.release();
      await run;
      await cleared;
      await settle();

      expect(await sync.serverIdOf('notes', 'a'), isNull);
      expect(await sync.pendingOperations(), isEmpty);
      expect(sync.status.value.lastSyncedAt, isNull);
      expect(events.last, SyncEventType.storeCleared);
    });

    test('engine calls made from inside a run do not deadlock', () async {
      late SyncEngine sync;
      final local = CallbackLocalAdapter(
        applyRemote: (r, {localId, required serverId}) async =>
            localId ?? serverId,
        onServerIdAssigned: (l, s, {serverRecord}) async {
          await sync.resetPullCursor();
          await sync.clear();
        },
      );
      sync = engine(local: local);
      await sync.recordCreate('notes', 'a', {});
      await sync.syncNow().timeout(const Duration(seconds: 2));
    });
  });

  group('status', () {
    test('an unchanged status does not notify listeners', () async {
      final sync = engine();
      await sync.recordCreate('notes', 'a', {'v': 1});
      var notifications = 0;
      sync.status.addListener(() => notifications++);
      await sync.recordUpdate('notes', 'a', {'v': 2}); // merged: same counts
      await sync.recordUpdate('notes', 'a', {'v': 3});
      expect(notifications, 0);
    });

    test('watchRecordState follows a record and stops on dispose', () async {
      final sync = engine();
      final states = <RecordSyncState>[];
      final done = Completer<void>();
      sync
          .watchRecordState('notes', 'a')
          .listen(states.add, onDone: done.complete);
      await settle();
      await sync.recordCreate('notes', 'a', {});
      await sync.recordUpdate('notes', 'a', {'x': 1});
      await settle();
      await sync.syncNow();
      await settle();
      await sync.dispose();
      await done.future.timeout(const Duration(seconds: 1));
      expect(states, [
        RecordSyncState.synced,
        RecordSyncState.pending,
        RecordSyncState.synced,
      ]);
    });
  });
}
