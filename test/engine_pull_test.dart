import 'dart:async';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

/// A remote whose pull answers come from [pages], keyed by cursor.
RemoteAdapter _pages(
  Map<String?, FutureOr<PullPage> Function()> pages, {
  List<String?>? cursors,
  Future<PushOutcome> Function(PushRequest request)? push,
}) =>
    CallbackRemoteAdapter(
      push: push ?? (_) async => const PushOutcome.success(serverId: '1'),
      pull: (request) async {
        cursors?.add(request.cursor);
        final page = pages[request.cursor];
        if (page == null) return const PullPage(records: []);
        return page();
      },
    );

void main() {
  late FakeServer server;
  late FakeLocalDb db;
  late InMemorySyncStore store;

  setUp(() {
    server = FakeServer();
    db = FakeLocalDb();
    store = InMemorySyncStore();
  });

  SyncEngine engine(
    List<SyncEntityConfig> entities, {
    SyncConfig config = manualConfig,
  }) {
    final sync = SyncEngine(store: store, entities: entities, config: config);
    addTearDown(sync.dispose);
    return sync;
  }

  test('parents are pulled first, whatever the registration order', () {
    final sync = engine([
      entity('payments', server, db,
          references: [SyncReference('billId', 'bills')]),
      entity('bills', server, db,
          references: [SyncReference('customerId', 'customers')]),
      entity('customers', server, db),
      SyncEntityConfig(
        name: 'tags',
        remote: server.adapter('tags'),
        local: db.adapter('tags'),
        dependsOn: ['payments'],
      ),
    ]);
    expect(sync.pullOrder, ['customers', 'bills', 'payments', 'tags']);
  });

  test('pulled records get local ids, references included', () async {
    final sync = engine([
      entity('bills', server, db, pullMode: PullMode.incremental, references: [
        SyncReference('customerId', 'customers'),
        SyncReference('lines[].itemId', 'items'),
      ]),
      entity('customers', server, db, pullMode: PullMode.incremental),
      entity('items', server, db, pullMode: PullMode.incremental),
    ]);
    final customer = server.seed('customers', {'name': 'Ann'});
    final item = server.seed('items', {'name': 'Tea'});
    server.seed('bills', {
      'customerId': int.parse(customer),
      'lines': [
        {'itemId': int.parse(item), 'qty': 2},
        {'itemId': 999, 'qty': 1}, // unknown on this device: kept as is
      ],
    });
    final pulled = <String>[];
    sync.events
        .where((e) => e.type == SyncEventType.recordPulled)
        .listen((e) => pulled.add(e.entity!));

    final result = await sync.syncNow();

    expect(result.pulled, 3);
    final customerLocal = await sync.localIdOf('customers', customer);
    final itemLocal = await sync.localIdOf('items', item);
    expect(db.table('customers')[customerLocal]!['name'], 'Ann');
    final bill = db.table('bills').values.single;
    expect(bill['customerId'], customerLocal);
    expect(bill['lines'], [
      {'itemId': itemLocal, 'qty': 2},
      {'itemId': 999, 'qty': 1},
    ]);
    expect(pulled, ['customers', 'items', 'bills']);
  });

  test('an incremental pull resumes from its cursor, even after a restart',
      () async {
    for (var i = 0; i < 5; i++) {
      server.seed('notes', {'n': i});
    }
    final first = engine([
      entity('notes', server, db,
          pullMode: PullMode.incremental, pullPageSize: 2),
    ]);
    expect((await first.pullNow()).pulled, 5);
    expect(server.pulls.map((p) => p.cursor), [null, '2', '4']);
    await first.dispose();

    server.seed('notes', {'n': 5});
    server.pulls.clear();
    final restarted = engine([
      entity('notes', server, db,
          pullMode: PullMode.incremental, pullPageSize: 2),
    ]);
    expect((await restarted.pullNow()).pulled, 1);
    expect(server.pulls.single.cursor, '5');

    await restarted.resetPullCursor();
    server.pulls.clear();
    await restarted.pullNow();
    expect(server.pulls.first.cursor, isNull);
    expect(db.table('notes'), hasLength(6)); // re-pulled, not duplicated
  });

  group('full refresh', () {
    test('reports every server id once all pages are in', () async {
      Set<String>? seen;
      final sync = engine([
        entity('items', server, db,
            pullMode: PullMode.fullRefresh,
            remote: _pages({
              null: () => const PullPage(records: [
                    {'id': 1},
                    {'id': 2},
                  ], hasMore: true, nextCursor: 'p2'),
              'p2': () => const PullPage(records: [
                    {'id': 3},
                  ]),
            }),
            local: CallbackLocalAdapter(
              applyRemote: (r, {localId, required serverId}) async =>
                  localId ?? 'l$serverId',
              onFullRefreshComplete: (ids) async => seen = ids,
            )),
      ]);
      await sync.pullNow();
      expect(seen, {'1', '2', '3'});
    });

    test('is not reported when the pull is cut short', () async {
      final reports = <Set<String>>[];
      Future<SyncRunResult> pull(
          Map<String?, FutureOr<PullPage> Function()> pages) {
        store = InMemorySyncStore();
        return engine([
          entity('items', server, db,
              pullMode: PullMode.fullRefresh,
              remote: _pages(pages),
              local: CallbackLocalAdapter(
                applyRemote: (r, {localId, required serverId}) async =>
                    localId ?? 'l$serverId',
                onFullRefreshComplete: (ids) async => reports.add(ids),
              )),
        ]).pullNow();
      }

      const first = PullPage(records: [
        {'id': 1},
      ], hasMore: true, nextCursor: 'p2');

      // More pages announced but no way to get them.
      final noCursor = await pull({
        null: () => const PullPage(records: [
              {'id': 1},
            ], hasMore: true),
      });
      expect(noCursor.errors.single, contains('hasMore without a new cursor'));
      // The second page fails.
      final failing = await pull({
        null: () => first,
        'p2': () => throw StateError('500'),
      });
      expect(failing.errors.single, contains('Pull items'));
      // The connection drops.
      final offline = await pull({
        null: () => first,
        'p2': () => throw const SyncNetworkException(),
      });
      expect(offline.abortedBy, SyncAbortReason.offline);

      expect(reports, isEmpty);
    });
  });

  test('server deletes remove the local row and its unpushed changes',
      () async {
    final sync = engine([
      entity('notes', server, db,
          pullMode: PullMode.incremental,
          isDeletedOf: (r) => r['deleted'] == true,
          remote: _pages({
            null: () => const PullPage(records: [
                  {'id': 100, 'deleted': true},
                ], deletedServerIds: [
                  '101',
                  '999', // never on this device
                ], nextCursor: 'c1'),
          })),
    ]);
    db.table('notes')
      ..['a'] = {'text': 'a'}
      ..['b'] = {'text': 'b'};
    await sync.registerMapping('notes', 'a', '100');
    await sync.registerMapping('notes', 'b', '101');
    await sync.recordUpdate('notes', 'b', {'text': 'edited'});
    final deleted = <String>[];
    sync.events
        .where((e) => e.type == SyncEventType.recordDeletedByServer)
        .listen((e) => deleted.add(e.localId!));

    await sync.pullNow();

    expect(db.table('notes'), isEmpty);
    expect(await sync.pendingOperations(), isEmpty);
    expect(deleted, ['a', 'b']);
  });

  group('pull conflicts', () {
    final older = DateTime.utc(2024, 1, 1);
    final newer = DateTime.utc(2030, 1, 1);

    Future<(SyncEngine, List<PushRequest>)> conflicted({
      ConflictStrategy strategy = ConflictStrategy.keepLocal,
      ConflictResolver? resolver,
      DateTime? serverTime,
    }) async {
      final pushes = <PushRequest>[];
      final sync = engine([
        SyncEntityConfig(
          name: 'notes',
          remote: _pages(
            {
              null: () => PullPage(records: [
                    {
                      'id': 100,
                      'text': 'server',
                      'tag': 's',
                      if (serverTime != null)
                        'updatedAt': serverTime.toIso8601String(),
                    },
                  ], nextCursor: 'c1'),
            },
            push: (r) async {
              pushes.add(r);
              return const PushOutcome.success();
            },
          ),
          local: db.adapter('notes'),
          conflictStrategy: strategy,
          conflictResolver: resolver,
          updatedAtOf: (r) => r['updatedAt'] == null
              ? null
              : DateTime.parse(r['updatedAt'] as String),
        ),
      ]);
      db.table('notes')['a'] = {'id': 100, 'text': 'old'};
      await sync.registerMapping('notes', 'a', '100');
      await sync.recordUpdate('notes', 'a', {'text': 'local'});
      return (sync, pushes);
    }

    test('keepLocal keeps the local row and forces the local change', () async {
      final (sync, pushes) = await conflicted();
      await sync.pullNow();
      expect(db.table('notes')['a']!['text'], 'old');
      await sync.pushNow();
      expect(pushes.single.force, isTrue);
      expect(pushes.single.payload, {'text': 'local'});
    });

    test('serverWins applies the server copy and drops the change', () async {
      final (sync, pushes) =
          await conflicted(strategy: ConflictStrategy.serverWins);
      await sync.pullNow();
      expect(db.table('notes')['a']!['text'], 'server');
      expect(await sync.pendingOperations(), isEmpty);
      await sync.pushNow();
      expect(pushes, isEmpty);
    });

    test('lastWriteWins picks the newer side', () async {
      final (serverNewer, _) = await conflicted(
          strategy: ConflictStrategy.lastWriteWins, serverTime: newer);
      await serverNewer.pullNow();
      expect(db.table('notes')['a']!['text'], 'server');

      store = InMemorySyncStore();
      db = FakeLocalDb();
      final (localNewer, _) = await conflicted(
          strategy: ConflictStrategy.lastWriteWins, serverTime: older);
      await localNewer.pullNow();
      expect(db.table('notes')['a']!['text'], 'old');
      expect(await localNewer.pendingOperations(), hasLength(1));
    });

    test('merge applies the merged record and queues it with force', () async {
      final (sync, pushes) = await conflicted(
          resolver: (c) => ConflictResolution.merge(
              {...c.serverRecord!, ...c.localPayload}));
      await sync.pullNow();
      expect(db.table('notes')['a'], {'id': 100, 'text': 'local', 'tag': 's'});
      await sync.pushNow();
      expect(pushes.single.force, isTrue);
      expect(pushes.single.payload['tag'], 's');
    });
  });

  group('failures', () {
    test('offline stops the pull; later entities wait', () async {
      final cursors = <String?>[];
      final sync = engine([
        entity('a', server, db,
            pullMode: PullMode.incremental,
            remote: _pages({null: () => throw const SyncNetworkException()})),
        entity('b', server, db,
            pullMode: PullMode.incremental,
            remote: _pages({}, cursors: cursors)),
      ]);
      final result = await sync.pullNow();
      expect(result.abortedBy, SyncAbortReason.offline);
      expect(cursors, isEmpty);
      expect(sync.status.value.phase, SyncPhase.offline);
    });

    test('unauthorized reports authRequired', () async {
      final sync = engine([
        entity('a', server, db,
            pullMode: PullMode.incremental,
            remote:
                _pages({null: () => throw const SyncUnauthorizedException()})),
      ]);
      await sync.pullNow();
      expect(sync.status.value.phase, SyncPhase.authRequired);
    });

    test('other errors are reported and the next entities still pull',
        () async {
      final sync = engine([
        entity('a', server, db,
            pullMode: PullMode.incremental,
            remote: _pages({null: () => throw StateError('bad page')})),
        entity('b', server, db,
            pullMode: PullMode.incremental,
            remote: _pages({
              null: () => const PullPage(records: [
                    {'id': 1},
                  ]),
            })),
      ]);
      final result = await sync.pullNow();
      expect(result.errors.single, contains('bad page'));
      expect(result.pulled, 1);
      expect(sync.status.value.lastError, contains('bad page'));
      expect(sync.status.value.lastSyncedAt, isNotNull);
    });

    test('a page that fails to apply is rolled back', () async {
      var calls = 0;
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.incremental,
            remote: _pages({
              null: () => const PullPage(records: [
                    {'id': 1},
                    {'id': 2},
                  ], nextCursor: 'c1'),
            }), local: CallbackLocalAdapter(
          applyRemote: (r, {localId, required serverId}) async {
            if (++calls == 2) throw StateError('disk full');
            return 'l$serverId';
          },
        )),
      ]);
      final result = await sync.pullNow();
      expect(result.errors.single, contains('disk full'));
      expect(result.pulled, 0);
      expect(await sync.localIdOf('notes', '1'), isNull);
    });
  });

  test('legacy rows are found through findLocalId', () async {
    final sync = engine([
      entity('notes', server, db,
          pullMode: PullMode.incremental,
          remote: _pages({
            null: () => const PullPage(records: [
                  {'id': 100, 'text': 'server'},
                ]),
          }),
          local: CallbackLocalAdapter(
            applyRemote: (r, {localId, required serverId}) async {
              db.table('notes')[localId ?? 'new'] = r;
              return localId ?? 'new';
            },
            findLocalId: (serverId) async => serverId == '100' ? '7' : null,
          )),
    ]);
    await sync.pullNow();
    expect(db.table('notes').keys, ['7']);
    expect(await sync.serverIdOf('notes', 7), '100');
  });

  test('syncNow pushes before it pulls', () async {
    final order = <String>[];
    final sync = engine([
      entity('notes', server, db,
          pullMode: PullMode.incremental,
          remote: CallbackRemoteAdapter(
            push: (r) async {
              order.add('push');
              return const PushOutcome.success(serverId: '1');
            },
            pull: (r) async {
              order.add('pull');
              return const PullPage(records: []);
            },
          )),
    ]);
    await sync.recordCreate('notes', 'a', {});
    await sync.syncNow();
    await sync.syncNow(pull: false);
    expect(order, ['push', 'pull']);
  });
}
