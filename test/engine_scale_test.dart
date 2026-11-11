import 'dart:async';
import 'dart:math';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import 'support/fakes.dart';

/// [manualConfig] with a few settings changed.
SyncConfig _config({
  int outboxPageSize = 200,
  int? serverUnavailableThreshold = 3,
  RetryPolicy retryPolicy = const RetryPolicy(jitter: 0),
  DateTime Function()? clock,
  FutureOr<bool> Function()? onUnauthorized,
  SyncLogger? logger,
}) =>
    SyncConfig(
      syncOnStart: false,
      syncOnResume: false,
      syncOnReconnect: false,
      autoPushAfterWrite: false,
      periodicInterval: null,
      outboxPageSize: outboxPageSize,
      serverUnavailableThreshold: serverUnavailableThreshold,
      retryPolicy: retryPolicy,
      clock: clock,
      onUnauthorized: onUnauthorized,
      logger: logger,
    );

/// A remote with a bulk endpoint. Every request made to it is listed in
/// [batches]; [answer] decides the outcome of each operation.
class _BulkRemote extends RemoteAdapter {
  final List<List<PushRequest>> batches = [];
  int singles = 0;
  int _nextId = 500;
  PushOutcome? Function(PushRequest request)? answer;
  Object? throwing;

  @override
  Future<PushOutcome> push(PushRequest request) async {
    singles++;
    return _one(request);
  }

  @override
  Future<List<PushOutcome>?> pushBatch(List<PushRequest> requests) async {
    batches.add(requests);
    final error = throwing;
    if (error != null) Error.throwWithStackTrace(error, StackTrace.current);
    return [for (final request in requests) _one(request)];
  }

  PushOutcome _one(PushRequest request) =>
      answer?.call(request) ??
      (request.type == SyncOpType.create
          ? PushOutcome.success(serverId: '${_nextId++}')
          : const PushOutcome.success());

  @override
  Future<PullPage> pull(PullRequest request) async =>
      const PullPage(records: []);

  List<List<String>> get sent => [
        for (final batch in batches)
          [for (final r in batch) '${r.type.name} ${r.localId}'],
      ];
}

/// A store whose [deleteOperation]s can be made to fail.
class _BrokenDeleteStore extends InMemorySyncStore {
  bool broken = false;

  @override
  Future<void> deleteOperation(String id) {
    if (broken) throw StateError('disk I/O error');
    return super.deleteOperation(id);
  }

  @override
  Future<void> deleteOperations(Iterable<String> ids) {
    if (broken) throw StateError('disk I/O error');
    return super.deleteOperations(ids);
  }
}

void main() {
  late FakeServer server;
  late FakeLocalDb db;
  late CountingStore store;

  setUp(() {
    server = FakeServer();
    db = FakeLocalDb();
    store = CountingStore();
  });

  SyncEngine engine(
    List<SyncEntityConfig> entities, {
    SyncConfig config = manualConfig,
    SyncStore? on,
  }) {
    final sync =
        SyncEngine(store: on ?? store, entities: entities, config: config);
    addTearDown(sync.dispose);
    return sync;
  }

  group('a large outbox', () {
    test('is pushed a page at a time, in order, without being loaded whole',
        () async {
      final sync = engine([entity('notes', server, db)],
          config: _config(outboxPageSize: 3));
      for (var i = 0; i < 10; i++) {
        await sync.recordCreate('notes', 'n$i', {'n': i});
      }
      final result = await sync.syncNow();

      expect(result.pushed, 10);
      expect(result.waiting, 0);
      expect(server.pushes.map((r) => r.localId),
          [for (var i = 0; i < 10; i++) 'n$i']);
      expect(sync.status.value.isUpToDate, isTrue);
      // Pages of three: 3 + 3 + 3 + 1, then the pass that finds nothing new.
      expect(store.count('operationsPage'), 5);
      expect(store.count('operations'), 0);
    });

    test('operations held back do not stop the pages behind them', () async {
      server.onPush =
          (r) => r.localId == 'n1' ? PushOutcome.retry('busy') : null;
      final sync = engine([entity('notes', server, db)],
          config: _config(outboxPageSize: 2));
      for (var i = 0; i < 7; i++) {
        await sync.recordCreate('notes', 'n$i', {'n': i});
      }
      final result = await sync.syncNow();
      expect(result.pushed, 6);
      expect(result.waiting, 1);
      expect((await sync.pendingOperations()).single.localId, 'n1');
      expect(sync.status.value.nextRetryAt, isNotNull);

      // The one waiting out its backoff is not even read by the next run.
      server.pushes.clear();
      final again = await sync.syncNow();
      expect(server.pushes, isEmpty);
      expect(again.waiting, 1);
    });

    test('a record keeps its order across pages when coalescing is off',
        () async {
      final sync = engine([entity('notes', server, db, coalesce: false)],
          config: _config(outboxPageSize: 2));
      await sync.recordCreate('notes', 'a', {'v': 1});
      await sync.recordCreate('notes', 'b', {'v': 1});
      await sync.recordUpdate('notes', 'a', {'v': 2});
      await sync.recordUpdate('notes', 'a', {'v': 3});
      await sync.recordUpdate('notes', 'b', {'v': 2});
      await sync.recordUpdate('notes', 'b', {'v': 3});
      final result = await sync.syncNow();
      expect(result.pushed, 6);
      expect(server.pushes.map((r) => '${r.type.name} ${r.localId}'), [
        'create a',
        'create b',
        'update a',
        'update a',
        'update b',
        'update b',
      ]);
      expect(server.table('notes').values.map((row) => row['v']), [3, 3]);
    });

    test('a change waits while an earlier one of its record is stuck',
        () async {
      var reject = true;
      server.onPush = (r) => reject && r.type == SyncOpType.create
          ? const PushOutcome.rejected('Invalid')
          : null;
      final sync = engine([entity('notes', server, db, coalesce: false)]);
      await sync.recordCreate('notes', 'a', {'v': 1});
      await sync.recordUpdate('notes', 'a', {'v': 2});
      final first = await sync.syncNow();
      expect(first.failed, 1);
      expect(server.pushes, hasLength(1));

      // The update is not sent ahead of the create it follows.
      await sync.syncNow();
      expect(server.pushes, hasLength(1));
      final ops = await sync.operationsOf('notes', 'a');
      expect(ops.map((o) => o.status),
          [SyncOpStatus.failed, SyncOpStatus.pending]);

      reject = false;
      await sync.retryFailed();
      final retried = await sync.syncNow();
      expect(retried.pushed, 2);
      expect(server.pushes.map((r) => r.type.name),
          ['create', 'create', 'update']);
    });

    test('pendingOperations and failedOperations can be read in pages',
        () async {
      server.onPush = (r) => r.localId.startsWith('bad')
          ? const PushOutcome.rejected('Invalid')
          : PushOutcome.networkError('offline');
      final sync = engine([entity('notes', server, db)]);
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'bad$i', {});
      }
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      // Every bad one is rejected, then the first good one stops the run.
      await sync.pushNow();
      expect(sync.status.value.failedCount, 5);

      final first = await sync.pendingOperations(limit: 2);
      expect(first.map((o) => o.localId), ['n0', 'n1']);
      final second =
          await sync.pendingOperations(limit: 2, afterSeq: first.last.seq!);
      expect(second.map((o) => o.localId), ['n2', 'n3']);
      final rest = await sync.pendingOperations(afterSeq: second.last.seq!);
      expect(rest.map((o) => o.localId), ['n4']);
      expect(await sync.pendingOperations(), hasLength(5));

      final failed = await sync.failedOperations(limit: 3);
      expect(failed.map((o) => o.localId), ['bad0', 'bad1', 'bad2']);
      expect(
          (await sync.failedOperations(afterSeq: failed.last.seq!))
              .map((o) => o.localId),
          ['bad3', 'bad4']);
    });

    test('retryFailed re-queues more failures than one page holds', () async {
      var reject = true;
      server.onPush =
          (r) => reject ? const PushOutcome.rejected('Invalid') : null;
      final sync = engine([entity('notes', server, db)],
          config: _config(outboxPageSize: 2));
      final events = <SyncEventType>[];
      sync.events.listen((e) => events.add(e.type));
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      await sync.syncNow();
      expect(sync.status.value.failedCount, 5);

      reject = false;
      await sync.retryFailed();
      expect(sync.status.value.failedCount, 0);
      expect(sync.status.value.pendingCount, 5);
      final ops = await sync.pendingOperations();
      expect(ops.every((o) => o.attempts == 0 && o.lastError == null), isTrue);
      await settle();
      expect(events.where((e) => e == SyncEventType.operationRetried),
          hasLength(5));
      expect((await sync.syncNow()).pushed, 5);
    });

    test('the status counts stay exact while thousands of changes queue up',
        () async {
      final sync = engine([entity('notes', server, db)]);
      const total = 4000;
      for (var i = 0; i < total; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      // A large outbox is no longer counted after every single change...
      expect(store.count('countByStatus'), lessThan(total));
      // ...but the count catches up by itself, and is exact after a run.
      await settle(const Duration(milliseconds: 200));
      expect(sync.status.value.pendingCount, total);

      server.onPush = (r) =>
          r.localId == 'n7' ? const PushOutcome.rejected('Invalid') : null;
      final result = await sync.syncNow();
      expect(result.pushed, total - 1);
      expect(sync.status.value.pendingCount, 0);
      expect(sync.status.value.failedCount, 1);
      expect(sync.status.value.runPushed, total - 1);
    });
  });

  group('batches', () {
    test('operations of one entity go to the bulk endpoint together', () async {
      final remote = _BulkRemote();
      final sync = engine(
          [entity('notes', server, db, remote: remote, pushBatchSize: 3)]);
      final pushed = <String>[];
      sync.events
          .where((e) => e.type == SyncEventType.operationPushed)
          .listen((e) => pushed.add('${e.localId}=${e.message}'));
      for (var i = 0; i < 7; i++) {
        await sync.recordCreate('notes', 'n$i', {'n': i});
      }
      final result = await sync.syncNow();

      expect(result.pushed, 7);
      expect(result.isSuccess, isTrue);
      expect(remote.singles, 0);
      expect(remote.sent, [
        ['create n0', 'create n1', 'create n2'],
        ['create n3', 'create n4', 'create n5'],
        ['create n6'],
      ]);
      expect(remote.batches.first.first.payload, {'n': 0});
      expect(remote.batches.first.first.idempotencyKey, isNotEmpty);
      expect(await sync.serverIdOf('notes', 'n0'), '500');
      expect(await sync.serverIdOf('notes', 'n6'), '506');
      expect(await sync.localIdOf('notes', '503'), 'n3');
      expect(db.serverIdsAssigned,
          [for (var i = 0; i < 7; i++) 'notes/n$i=${500 + i}']);
      expect(db.pushed, [for (var i = 0; i < 7; i++) 'create notes/n$i']);
      expect(await sync.pendingOperations(), isEmpty);
      expect(sync.status.value.isUpToDate, isTrue);
      expect(sync.status.value.runPushed, 7);
      await settle();
      expect(pushed, [for (var i = 0; i < 7; i++) 'n$i=${500 + i}']);
    });

    test('each operation of a batch gets its own outcome', () async {
      final remote = _BulkRemote()
        ..answer = (r) => switch (r.localId) {
              'rejected' => const PushOutcome.rejected('Invalid'),
              'busy' => PushOutcome.retry('busy'),
              'conflict' => const PushOutcome.conflict(error: 'stale'),
              'noid' => const PushOutcome.success(),
              _ => null,
            };
      final sync = engine([
        entity('notes', server, db,
            remote: remote,
            pushBatchSize: 10,
            conflictStrategy: ConflictStrategy.serverWins),
      ]);
      for (final id in ['a', 'rejected', 'busy', 'conflict', 'noid', 'b']) {
        await sync.recordCreate('notes', id, {'id': id});
      }
      final result = await sync.syncNow();

      expect(remote.batches, hasLength(1));
      expect(result.pushed, 2);
      expect(result.failed, 2);
      expect(result.waiting, 1);
      expect(await sync.serverIdOf('notes', 'a'), isNotNull);
      expect(await sync.serverIdOf('notes', 'b'), isNotNull);
      expect(
          await sync.recordState('notes', 'rejected'), RecordSyncState.failed);
      expect(await sync.recordState('notes', 'noid'), RecordSyncState.failed);
      // The conflict was resolved for the server: its change is dropped.
      expect(await sync.operationsOf('notes', 'conflict'), isEmpty);
      final busy = (await sync.operationsOf('notes', 'busy')).single;
      expect(busy.status, SyncOpStatus.pending);
      expect(busy.attempts, 1);
      expect(busy.nextAttemptAt, isNotNull);
      expect(sync.status.value.pendingCount, 1);
      expect(sync.status.value.failedCount, 2);
    });

    test('a batch holds one entity and one change per record', () async {
      final remote = _BulkRemote();
      final sync = engine([
        entity('notes', server, db,
            remote: remote, pushBatchSize: 10, coalesce: false),
        entity('tags', server, db, remote: remote, pushBatchSize: 10),
      ]);
      await sync.recordCreate('notes', 'a', {'v': 1});
      await sync.recordCreate('notes', 'b', {'v': 1});
      await sync.recordCreate('tags', 't', {});
      await sync.recordCreate('notes', 'c', {'v': 1});
      await sync.recordUpdate('notes', 'a', {'v': 2});
      await sync.recordUpdate('notes', 'a', {'v': 3});
      await sync.recordUpdate('notes', 'b', {'v': 2});
      final result = await sync.syncNow();

      expect(result.pushed, 7);
      expect(remote.sent, [
        ['create a', 'create b'],
        ['create t'],
        ['create c', 'update a'],
        ['update a', 'update b'],
      ]);
      // The updates carry the ids their creates were given.
      expect(remote.batches[2][1].serverId, '500');
      expect(remote.batches[3][1].serverId, '501');
    });

    test('without a bulk endpoint a batch is sent side by side', () async {
      var inFlight = 0;
      var most = 0;
      var next = 100;
      final remote = CallbackRemoteAdapter(push: (request) async {
        inFlight++;
        most = max(most, inFlight);
        await Future<void>.delayed(const Duration(milliseconds: 5));
        inFlight--;
        if (request.localId == 'n5') throw const SyncRejectedException('no');
        return PushOutcome.success(serverId: '${next++}');
      });
      final sync = engine(
          [entity('notes', server, db, remote: remote, pushBatchSize: 4)]);
      for (var i = 0; i < 10; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();

      expect(most, 4);
      expect(result.pushed, 9);
      expect(result.failed, 1);
      expect(await sync.recordState('notes', 'n5'), RecordSyncState.failed);
      expect(await sync.serverIdOf('notes', 'n9'), isNotNull);
    });

    test('one at a time is still the default', () async {
      var inFlight = 0;
      var most = 0;
      var next = 100;
      final remote = CallbackRemoteAdapter(
        push: (request) async {
          inFlight++;
          most = max(most, inFlight);
          await Future<void>.delayed(const Duration(milliseconds: 1));
          inFlight--;
          return PushOutcome.success(serverId: '${next++}');
        },
        pushBatch: (requests) async => fail('pushBatch was called'),
      );
      final sync = engine([entity('notes', server, db, remote: remote)]);
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      expect((await sync.syncNow()).pushed, 5);
      expect(most, 1);
    });

    test('a record waits for another one of the same batch it references',
        () async {
      final remote = _BulkRemote();
      final sync = engine([
        entity('folders', server, db,
            remote: remote,
            pushBatchSize: 10,
            references: [SyncReference('parentId', 'folders')]),
      ]);
      await sync.recordCreate('folders', 'root', {'name': 'Root'});
      await sync.recordCreate(
          'folders', 'docs', {'name': 'Docs', 'parentId': 'root'});
      await sync.recordCreate(
          'folders', 'work', {'name': 'Work', 'parentId': 'docs'});
      await sync.recordCreate('folders', 'other', {'name': 'Other'});
      final result = await sync.syncNow();

      // One run; a parent always travels before its child.
      expect(result.pushed, 4);
      expect(result.waiting, 0);
      expect(remote.sent, [
        ['create root'],
        ['create docs'],
        ['create work', 'create other'],
      ]);
      expect(remote.batches[1].single.payload['parentId'], '500');
      expect(remote.batches[2].first.payload['parentId'], '501');
    });

    test('references to other entities are resolved for the whole batch',
        () async {
      final remote = _BulkRemote();
      final sync = engine([
        entity('customers', server, db, remote: remote, pushBatchSize: 10),
        entity('bills', server, db,
            remote: remote,
            pushBatchSize: 10,
            references: [
              SyncReference('customerId', 'customers'),
              SyncReference('items[].productId', 'products'),
            ]),
        entity('products', server, db, remote: remote),
      ]);
      await sync.registerMapping('products', 'p1', '900');
      await sync.recordCreate('customers', 'c1', {'name': 'A'});
      await sync.recordCreate('customers', 'c2', {'name': 'B'});
      for (var i = 0; i < 4; i++) {
        await sync.recordCreate('bills', 'b$i', {
          'customerId': i.isEven ? 'c1' : 'c2',
          'items': [
            {'productId': 'p1', 'qty': i},
          ],
        });
      }
      store.calls.clear();
      final result = await sync.syncNow();

      expect(result.pushed, 6);
      expect(remote.sent, [
        ['create c1', 'create c2'],
        ['create b0', 'create b1', 'create b2', 'create b3'],
      ]);
      expect(remote.batches[1].map((r) => r.payload['customerId']),
          ['500', '501', '500', '501']);
      expect(remote.batches[1].first.payload['items'], [
        {'productId': '900', 'qty': 0},
      ]);
      // Looked up per batch, not per operation.
      expect(store.count('mappingForLocal'), 0);
      expect(store.count('putMapping'), 0);
    });

    test('a batch the server cannot be reached for is put back untouched',
        () async {
      final remote = _BulkRemote()
        ..throwing = const SyncNetworkException('no route');
      final sync = engine(
          [entity('notes', server, db, remote: remote, pushBatchSize: 5)]);
      for (var i = 0; i < 8; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();

      expect(result.abortedBy, SyncAbortReason.offline);
      expect(result.pushed, 0);
      expect(remote.batches, hasLength(1));
      final ops = await sync.pendingOperations();
      expect(ops, hasLength(8));
      expect(
          ops.every((o) =>
              o.status == SyncOpStatus.pending &&
              o.attempts == 0 &&
              o.nextAttemptAt == null),
          isTrue);
      expect(sync.status.value.phase, SyncPhase.offline);

      remote.throwing = null;
      expect((await sync.syncNow()).pushed, 8);
    });

    test('a bulk endpoint that throws or miscounts backs the batch off',
        () async {
      final remote = _BulkRemote()..throwing = StateError('bad gateway');
      final sync = engine(
          [entity('notes', server, db, remote: remote, pushBatchSize: 5)]);
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      final result = await sync.syncNow();
      expect(result.pushed, 0);
      expect(result.waiting, 2);
      var ops = await sync.pendingOperations();
      expect(ops.map((o) => o.attempts), [1, 1]);
      expect(ops.first.lastError, contains('bad gateway'));

      final miscounting = CallbackRemoteAdapter(
        push: (r) async => const PushOutcome.success(serverId: '1'),
        pushBatch: (requests) async =>
            [const PushOutcome.success(serverId: '1')],
      );
      final other = engine([
        entity('notes', server, db, remote: miscounting, pushBatchSize: 5),
      ], on: InMemorySyncStore());
      await other.recordCreate('notes', 'a', {});
      await other.recordCreate('notes', 'b', {});
      await other.syncNow();
      ops = await other.pendingOperations();
      expect(ops, hasLength(2));
      expect(ops.first.lastError, contains('pushBatch returned 1 outcomes'));
      expect(await other.serverIdOf('notes', 'a'), isNull);
    });

    test('bookkeeping that fails for one record does not undo the others',
        () async {
      final remote = _BulkRemote();
      final pushedCalls = <String>[];
      final local = CallbackLocalAdapter(
        applyRemote: (r, {localId, required serverId}) async =>
            localId ?? serverId,
        onPushed: (op, {serverId}) async {
          pushedCalls.add(op.localId);
          if (op.localId == 'n1') throw StateError('constraint failed');
        },
      );
      final sync = engine([
        entity('notes', server, db,
            remote: remote, local: local, pushBatchSize: 5),
      ]);
      for (var i = 0; i < 3; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();

      // The server has all three: none may be sent again.
      expect(result.pushed, 3);
      expect(result.errors.single, contains('notes/n1'));
      expect(await sync.pendingOperations(), isEmpty);
      for (var i = 0; i < 3; i++) {
        expect(await sync.serverIdOf('notes', 'n$i'), '${500 + i}');
      }
      expect(pushedCalls, containsAll(<String>['n0', 'n1', 'n2']));
      await sync.syncNow();
      expect(remote.batches, hasLength(1));
    });

    test('an expired session is refreshed once for the whole batch', () async {
      var token = 'old';
      var refreshes = 0;
      final remote = _BulkRemote()
        ..answer =
            (r) => token == 'old' ? const PushOutcome.unauthorized() : null;
      final sync = engine(
        [entity('notes', server, db, remote: remote, pushBatchSize: 4)],
        config: _config(onUnauthorized: () {
          refreshes++;
          token = 'new';
          return true;
        }),
      );
      for (var i = 0; i < 4; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();
      expect(refreshes, 1);
      expect(result.pushed, 4);
      expect(result.abortedBy, isNull);
      expect(remote.batches, hasLength(2));
    });

    test('works the same on SqlSyncStore', () async {
      final database = await openTestDatabase();
      addTearDown(database.close);
      final remote = _BulkRemote()
        ..answer = (r) =>
            r.localId == 'n3' ? const PushOutcome.rejected('Invalid') : null;
      final sync = engine([
        entity('notes', server, db, remote: remote, pushBatchSize: 4),
      ], on: SqlSyncStore(SqfliteExecutor(database)));
      for (var i = 0; i < 10; i++) {
        await sync.recordCreate('notes', 'n$i', {'n': i});
      }
      final result = await sync.syncNow();
      expect(result.pushed, 9);
      expect(result.failed, 1);
      expect(remote.batches.map((b) => b.length), [4, 4, 2]);
      expect(await sync.serverIdOf('notes', 'n9'), isNotNull);
      expect((await sync.failedOperations()).single.localId, 'n3');
      expect(sync.status.value.pendingCount, 0);
    });
  });

  group('a server that is unavailable', () {
    test('stops the run, keeps the rest untouched, and is left alone',
        () async {
      var now = DateTime(2024, 1, 1, 12);
      var down = true;
      server.onPush = (r) =>
          down ? PushOutcome.retry('HTTP 503', serverUnavailable: true) : null;
      final sync = engine([entity('notes', server, db)],
          config: _config(clock: () => now));
      for (var i = 0; i < 10; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();

      expect(result.abortedBy, SyncAbortReason.serverUnavailable);
      expect(result.completed, isFalse);
      expect(server.pushes, hasLength(3));
      expect(sync.status.value.phase, SyncPhase.serverUnavailable);
      expect(
          sync.status.value.lastError, contains('The server is unavailable'));
      expect(
          sync.status.value.nextRetryAt, now.add(const Duration(seconds: 2)));
      expect(sync.status.value.lastSyncedAt, isNull);
      var ops = await sync.pendingOperations();
      expect(ops.map((o) => o.attempts), [1, 1, 1, 0, 0, 0, 0, 0, 0, 0]);
      expect(ops.skip(3).every((o) => o.nextAttemptAt == null), isTrue);

      // During the pause nothing at all is sent.
      now = now.add(const Duration(seconds: 1));
      final paused = await sync.syncNow();
      expect(paused.abortedBy, SyncAbortReason.serverUnavailable);
      expect(server.pushes, hasLength(3));
      expect(sync.status.value.phase, SyncPhase.serverUnavailable);
      expect(
          sync.status.value.lastError, contains('The server is unavailable'));

      // A sync the user asks for tries at once; one answer is then enough
      // to pause again, for longer.
      final forced = await sync.syncNow(ignoreBackoff: true);
      expect(forced.abortedBy, SyncAbortReason.serverUnavailable);
      expect(server.pushes, hasLength(4));
      expect(
          sync.status.value.nextRetryAt, now.add(const Duration(seconds: 4)));

      // After the pause, a single operation probes the server.
      now = now.add(const Duration(seconds: 5));
      await sync.syncNow();
      expect(server.pushes, hasLength(5));
      expect(
          sync.status.value.nextRetryAt, now.add(const Duration(seconds: 8)));

      // The server is back: everything goes through and the pause is over.
      down = false;
      now = now.add(const Duration(minutes: 1));
      final recovered = await sync.syncNow();
      expect(recovered.pushed, 10);
      expect(recovered.isSuccess, isTrue);
      expect(sync.status.value.phase, SyncPhase.idle);
      expect(sync.status.value.nextRetryAt, isNull);
      expect(sync.status.value.lastError, isNull);
      ops = await sync.pendingOperations();
      expect(ops, isEmpty);

      // ...and a later outage needs three answers again.
      down = true;
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'm$i', {});
      }
      server.pushes.clear();
      await sync.syncNow();
      expect(server.pushes, hasLength(3));
    });

    test('honours a longer Retry-After', () async {
      final now = DateTime(2024, 1, 1, 12);
      server.onPush = (r) => PushOutcome.retry('HTTP 429',
          retryAfter: const Duration(minutes: 2), serverUnavailable: true);
      final sync = engine([entity('notes', server, db)],
          config: _config(clock: () => now));
      for (var i = 0; i < 4; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      await sync.syncNow();
      expect(
          sync.status.value.nextRetryAt, now.add(const Duration(minutes: 2)));
    });

    test('ordinary failures in between do not add up', () async {
      var n = 0;
      // Two unavailable answers, then a plain server error, and so on.
      server.onPush = (r) => PushOutcome.retry(
          'HTTP ${n++ % 3 == 2 ? 500 : 503}',
          serverUnavailable: n % 3 != 0);
      final sync = engine([entity('notes', server, db)]);
      for (var i = 0; i < 9; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();
      expect(result.abortedBy, isNull);
      expect(server.pushes, hasLength(9));
    });

    test('a server error about one record never stops the run', () async {
      server.onPush = (r) => PushOutcome.retry('HTTP 500');
      final sync = engine([entity('notes', server, db)]);
      for (var i = 0; i < 6; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();
      expect(result.abortedBy, isNull);
      expect(server.pushes, hasLength(6));
    });

    test('the threshold can be turned off', () async {
      server.onPush =
          (r) => PushOutcome.retry('HTTP 503', serverUnavailable: true);
      final sync = engine([entity('notes', server, db)],
          config: _config(serverUnavailableThreshold: null));
      for (var i = 0; i < 6; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();
      expect(result.abortedBy, isNull);
      expect(server.pushes, hasLength(6));
      expect(sync.status.value.phase, SyncPhase.idle);
    });

    test('sync resumes by itself when the pause ends, pull included', () async {
      var failures = 3;
      server.onPush = (r) => failures-- > 0
          ? PushOutcome.retry('HTTP 503', serverUnavailable: true)
          : null;
      final sync = engine(
        [entity('notes', server, db, pullMode: PullMode.incremental)],
        config: const SyncConfig(
          syncOnStart: false,
          syncOnResume: false,
          syncOnReconnect: false,
          autoPushAfterWrite: false,
          periodicInterval: null,
          retryPolicy:
              RetryPolicy(baseDelay: Duration(milliseconds: 40), jitter: 0),
        ),
      );
      await sync.start();
      for (var i = 0; i < 5; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final result = await sync.syncNow();
      expect(result.abortedBy, SyncAbortReason.serverUnavailable);
      expect(server.pulls, isEmpty);

      await settle(const Duration(milliseconds: 400));
      expect(sync.status.value.isUpToDate, isTrue);
      expect(sync.status.value.phase, SyncPhase.idle);
      expect(server.table('notes'), hasLength(5));
      // The pull the pause prevented was made as well.
      expect(server.pulls, isNotEmpty);
    });
  });

  group('cancelling', () {
    test('cancelSync stops a push after the operation being sent', () async {
      final gate = Completer<void>();
      final started = Completer<void>();
      var sent = 0;
      final remote = CallbackRemoteAdapter(push: (request) async {
        sent++;
        if (sent == 1) {
          started.complete();
          await gate.future;
        }
        return PushOutcome.success(serverId: '${100 + sent}');
      });
      final sync = engine([entity('notes', server, db, remote: remote)]);
      for (var i = 0; i < 4; i++) {
        await sync.recordCreate('notes', 'n$i', {});
      }
      final run = sync.pushNow();
      await started.future;
      final queued = sync.syncNow();
      final cancelled = sync.cancelSync();
      gate.complete();
      await cancelled;

      final result = await run;
      expect(result.abortedBy, SyncAbortReason.cancelled);
      expect(result.pushed, 1);
      expect((await queued).abortedBy, SyncAbortReason.cancelled);
      expect(sent, 1);
      final left = await sync.pendingOperations();
      expect(left.map((o) => o.status), List.filled(3, SyncOpStatus.pending));
      expect(sync.status.value.phase, SyncPhase.idle);
      expect(sync.status.value.lastSyncedAt, isNull);

      // Nothing was lost: the next sync carries on.
      expect((await sync.syncNow()).pushed, 3);
      expect(sync.status.value.lastSyncedAt, isNotNull);
      // With nothing running it does nothing.
      await sync.cancelSync();
    });

    test('cancelSync stops a pull between pages; the next one resumes',
        () async {
      for (var i = 0; i < 9; i++) {
        server.seed('notes', {'n': i});
      }
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.incremental, pullPageSize: 2),
      ]);
      late final StreamSubscription<SyncStatus> watching;
      watching = sync.watchStatus().listen((s) {
        if (s.runPulled >= 4) {
          sync.cancelSync();
          watching.cancel();
        }
      });
      final result = await sync.pullNow();

      expect(result.abortedBy, SyncAbortReason.cancelled);
      expect(result.pulled, lessThan(9));
      expect(result.pulled, greaterThanOrEqualTo(4));
      final pulledSoFar = db.table('notes').length;
      expect(pulledSoFar, result.pulled);

      // Pages already written are not requested again.
      server.pulls.clear();
      final resumed = await sync.pullNow();
      expect(resumed.pulled, 9 - pulledSoFar);
      expect(server.pulls.first.cursor, '$pulledSoFar');
      expect(db.table('notes'), hasLength(9));
    });

    test('clear does not wait for a long pull to finish', () async {
      for (var i = 0; i < 40; i++) {
        server.seed('notes', {'n': i});
      }
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.incremental, pullPageSize: 2),
      ]);
      final run = sync.pullNow();
      await settle();
      await sync.clear();

      final result = await run;
      expect(result.abortedBy, SyncAbortReason.cancelled);
      expect(server.pulls.length, lessThan(20));
      expect(await sync.localIdOf('notes', '100'), isNull);
      expect(sync.status.value, const SyncStatus());
    });

    test('dispose stops a pull between pages', () async {
      for (var i = 0; i < 40; i++) {
        server.seed('notes', {'n': i});
      }
      final sync = SyncEngine(
        store: store,
        config: manualConfig,
        entities: [
          entity('notes', server, db,
              pullMode: PullMode.incremental, pullPageSize: 2),
        ],
      );
      final run = sync.pullNow();
      await settle();
      await sync.dispose();
      final result = await run;
      expect(result.abortedBy, SyncAbortReason.cancelled);
      expect(server.pulls.length, lessThan(20));
    });
  });

  group('robustness', () {
    test(
        'a write recorded inside a database transaction is still pushed '
        'automatically', () async {
      final database = await openTestDatabase();
      addTearDown(database.close);
      await database
          .execute('CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)');
      final executor = CallbackSqlExecutor<DatabaseExecutor>(
        connection: database,
        execute: (c, sql, args) => c.execute(sql, args),
        query: (c, sql, args) => c.rawQuery(sql, args),
        transaction: (action) => database.transaction(action),
      );
      final logs = <String>[];
      var failures = 1;
      server.onPush = (r) => failures-- > 0 ? PushOutcome.retry('busy') : null;
      final sync = engine(
        [entity('notes', server, db)],
        on: SqlSyncStore(executor),
        config: SyncConfig(
          syncOnStart: false,
          syncOnResume: false,
          periodicInterval: null,
          debounce: const Duration(milliseconds: 20),
          retryPolicy: const RetryPolicy(
              baseDelay: Duration(milliseconds: 30), jitter: 0),
          logger: (level, message, [error, stackTrace]) {
            if (level.index >= SyncLogLevel.warning.index) {
              logs.add('$message ${error ?? ''}');
            }
          },
        ),
      );
      await sync.start();

      // The documented way to record a change: with the row, in one
      // transaction. The sync it triggers runs after the transaction ended.
      await executor.transaction(() async {
        await executor.current.insert('notes', {'id': 'a', 'body': 'hello'});
        await sync.recordCreate('notes', 'a', {'body': 'hello'});
      });
      await settle(const Duration(milliseconds: 400));

      // Pushed, and retried after its backoff, without touching the
      // transaction that is long over.
      expect(logs.where((l) => l.contains('transaction')), isEmpty);
      expect(server.pushes, hasLength(2));
      expect(server.table('notes').values.single['body'], 'hello');
      expect(sync.status.value.isUpToDate, isTrue);
      expect(await sync.serverIdOf('notes', 'a'), isNotNull);
    });

    test('a store failure in the middle of a push leaves nothing in flight',
        () async {
      final broken = _BrokenDeleteStore();
      final sync = engine([entity('notes', server, db)], on: broken);
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      broken.broken = true;
      final result = await sync.syncNow();

      expect(result.errors, isNotEmpty);
      expect(result.isSuccess, isFalse);
      final ops = await broken.operations();
      expect(ops.map((o) => o.status), everyElement(SyncOpStatus.pending));

      // Once the store works again the queue drains; the idempotency key
      // of the change the server already has is the same as before.
      broken.broken = false;
      final key = server.pushes.first.idempotencyKey;
      final retried = await sync.syncNow();
      expect(retried.errors, isEmpty);
      expect(await sync.pendingOperations(), isEmpty);
      expect(server.pushes.where((r) => r.idempotencyKey == key),
          hasLength(greaterThanOrEqualTo(1)));
      expect(sync.status.value.isUpToDate, isTrue);
    });

    test('registerMappings releases records waiting for another push',
        () async {
      final sync = engine([
        entity('signups', server, db),
        entity('customers', server, db),
        entity('bills', server, db,
            references: [SyncReference('customerId', 'customers')]),
      ]);
      await sync.recordCreate('signups', 's1', {'name': 'A'});
      await sync.recordCreatedVia('customers', 'c1',
          viaEntity: 'signups', viaLocalId: 's1');
      await sync.recordCreate('bills', 'b1', {'customerId': 'c1'});
      // Rows that existed before, registered in bulk with the new one.
      await sync.registerMappings('customers', {
        'c1': '700',
        for (var i = 0; i < 50; i++) 'old$i': '${800 + i}',
      });
      final result = await sync.syncNow();
      expect(result.pushed, 2);
      expect(server.pushes.last.payload['customerId'], '700');
      expect(await sync.localIdOf('customers', '849'), 'old49');
    });

    test('progress of a run is reported and starts again with the next one',
        () async {
      for (var i = 0; i < 5; i++) {
        server.seed('notes', {'n': i});
      }
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.fullRefresh, pullPageSize: 2),
        entity('tags', server, db),
      ]);
      await sync.recordCreate('tags', 't1', {});
      await sync.recordCreate('tags', 't2', {});
      final pulled = <int>[];
      final pushed = <int>[];
      sync.status.addListener(() {
        final s = sync.status.value;
        if (pulled.isEmpty || pulled.last != s.runPulled) {
          pulled.add(s.runPulled);
        }
        if (pushed.isEmpty || pushed.last != s.runPushed) {
          pushed.add(s.runPushed);
        }
      });
      await sync.syncNow();
      expect(pushed, [0, 1, 2]);
      expect(pulled, [0, 2, 4, 5]);
      expect(sync.status.value.runPulled, 5);
      expect(sync.status.value.runPushed, 2);

      await sync.pushNow();
      expect(sync.status.value.runPulled, 0);
      expect(sync.status.value.runPushed, 0);
    });
  });
}
