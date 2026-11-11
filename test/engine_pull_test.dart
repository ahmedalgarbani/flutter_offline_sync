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
    final dropped = <SyncEvent>[];
    sync.events.listen((e) {
      switch (e.type) {
        case SyncEventType.recordDeletedByServer:
          deleted.add(e.localId!);
        case SyncEventType.operationDiscarded:
          dropped.add(e);
        default:
      }
    });

    await sync.pullNow();
    await settle();

    expect(db.table('notes'), isEmpty);
    expect(await sync.pendingOperations(), isEmpty);
    expect(deleted, ['a', 'b']);
    // The lost local edit is reported, so the app can tell the user.
    expect(dropped.single.localId, 'b');
    expect(dropped.single.message, contains('deleted on the server'));
  });

  group('own records seen in a pull (clientIdOf)', () {
    // The server saves the create but answers 502; a pull runs before the
    // create is retried.
    Future<SyncEngine> lostResponse({bool echoClientId = true}) async {
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.fullRefresh,
            clientIdOf: echoClientId ? (r) => r['clientId'] : null),
      ]);
      var first = true;
      server.onPush = (r) {
        if (!first) return null;
        first = false;
        server.seed('notes', {...r.payload});
        return const PushOutcome.retry('502 Bad Gateway');
      };
      db.table('notes')['a'] = {'text': 'hi', 'clientId': 'a'};
      await sync.recordCreate('notes', 'a', {'text': 'hi', 'clientId': 'a'});
      return sync;
    }

    test('are linked to the local record instead of stored twice', () async {
      final sync = await lostResponse();
      final result = await sync.syncNow();

      expect(db.table('notes').keys, ['a']);
      expect(await sync.serverIdOf('notes', 'a'), '100');
      expect(db.serverIdsAssigned, ['notes/a=100']);
      expect(result.pulled, 0); // the local copy won (keepLocal)

      // The queued create now goes out as an update of record 100.
      await sync.syncNow(ignoreBackoff: true);
      expect(server.pushes.last.type, SyncOpType.update);
      expect(server.pushes.last.serverId, '100');
      expect(server.table('notes'), hasLength(1));
      expect(await sync.pendingOperations(), isEmpty);
    });

    test('without clientIdOf the record is stored twice', () async {
      final sync = await lostResponse(echoClientId: false);
      await sync.syncNow();
      expect(db.table('notes'), hasLength(2));
    });

    test('never take over a record that already has a server id', () async {
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.fullRefresh, clientIdOf: (r) => r['clientId']),
      ]);
      await sync.registerMapping('notes', 'a', '7');
      server.seed('notes', {'text': 'other', 'clientId': 'a'});
      await sync.pullNow();
      expect(await sync.serverIdOf('notes', 'a'), '7');
      expect(db.table('notes'), hasLength(1));
      expect(db.table('notes').keys.single, isNot('a'));
    });
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

    test('onUnauthorized lets the pull retry the page', () async {
      var token = 'old';
      final sync = engine(
        [
          entity('a', server, db,
              pullMode: PullMode.incremental,
              remote: _pages({
                null: () => token == 'old'
                    ? throw const SyncUnauthorizedException()
                    : const PullPage(records: [
                        {'id': 100},
                      ], nextCursor: 'c1'),
              })),
        ],
        config: SyncConfig(
          syncOnStart: false,
          syncOnResume: false,
          autoPushAfterWrite: false,
          periodicInterval: null,
          onUnauthorized: () {
            token = 'new';
            return true;
          },
        ),
      );
      final result = await sync.pullNow();
      expect(result.completed, isTrue);
      expect(result.pulled, 1);
      expect(sync.status.value.phase, SyncPhase.idle);
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

  group('whole pages', () {
    /// A local table written a page at a time; [batches] lists the server
    /// ids of each call.
    LocalAdapter batching(
      String name,
      List<List<String>> batches, {
      List<String>? Function(List<RemoteChange> changes)? answer,
    }) =>
        CallbackLocalAdapter(
          applyRemote: (record, {localId, required serverId}) =>
              fail('applyRemote was called for $serverId'),
          applyRemoteBatch: (changes) async {
            batches.add([for (final c in changes) c.serverId]);
            final ids = <String>[];
            for (final change in changes) {
              final id = change.localId ?? 'local-${change.serverId}';
              db.table(name)[id] = Map.of(change.record);
              ids.add(id);
            }
            return answer?.call(changes) ?? ids;
          },
          applyRemoteDelete: (localId, {required serverId}) async {
            db.table(name).remove(localId);
          },
        );

    test('a page is looked up and written as a whole', () async {
      for (var i = 0; i < 5; i++) {
        server.seed('customers', {'name': 'C$i'});
      }
      for (var i = 0; i < 7; i++) {
        server.seed('bills', {'customerId': 100 + i % 5, 'total': i});
      }
      final counting = CountingStore();
      final batches = <List<String>>[];
      final sync = SyncEngine(
        store: counting,
        config: manualConfig,
        entities: [
          entity('bills', server, db,
              pullMode: PullMode.incremental,
              pullPageSize: 3,
              local: batching('bills', batches),
              references: [SyncReference('customerId', 'customers')]),
          entity('customers', server, db,
              pullMode: PullMode.incremental, pullPageSize: 3),
        ],
      );
      addTearDown(sync.dispose);
      final pulled = <String>[];
      sync.events
          .where((e) => e.type == SyncEventType.recordPulled)
          .listen((e) => pulled.add('${e.entity}/${e.localId}=${e.message}'));
      final result = await sync.pullNow();
      final calls = Map.of(counting.calls);

      expect(result.pulled, 12);
      expect(result.errors, isEmpty);
      expect(batches, [
        ['105', '106', '107'],
        ['108', '109', '110'],
        ['111'],
      ]);
      final bill = db.table('bills')['local-108']!;
      expect(bill['customerId'], await sync.localIdOf('customers', '103'));
      expect(await sync.localIdOf('bills', '111'), 'local-111');
      expect(await sync.serverIdOf('bills', 'local-105'), '105');
      await settle();
      expect(pulled, hasLength(12));
      expect(pulled.last, 'bills/local-111=111');

      // Five pages were applied: the store is asked per page (once for the
      // page's own ids, once per referenced entity), never per record.
      expect(calls['mappingForServer'], isNull);
      expect(calls['mappingForLocal'], isNull);
      expect(calls['operations'], isNull);
      expect(calls['operationsFor'], isNull);
      expect(calls['putMapping'], isNull);
      expect(calls['putMappings'], 5);
      expect(calls['localIdsWithOperations'], 5);
      expect(calls['localIdsForServer'], 2 + 3 + 3);

      // Pulled again, every row is known and updated in place.
      batches.clear();
      await sync.resetPullCursor('bills');
      final again = await sync.pullNow(entities: {'bills'});
      expect(again.pulled, 7);
      expect(db.table('bills'), hasLength(7));
      expect(batches.first, ['105', '106', '107']);
    });

    test('the default writes a page record by record', () async {
      for (var i = 0; i < 5; i++) {
        server.seed('notes', {'n': i});
      }
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.incremental, pullPageSize: 2),
      ]);
      expect((await sync.pullNow()).pulled, 5);
      expect(db.table('notes').values.map((r) => r['n']), [0, 1, 2, 3, 4]);
    });

    test('a batch that returns the wrong number of ids is rolled back',
        () async {
      server.seed('notes', {'n': 0});
      server.seed('notes', {'n': 1});
      final batches = <List<String>>[];
      final sync = engine([
        entity('notes', server, db,
            pullMode: PullMode.incremental,
            local: batching('notes', batches, answer: (c) => ['only-one'])),
      ]);
      final result = await sync.pullNow();
      expect(result.pulled, 0);
      expect(result.errors.single, contains('applyRemoteBatch returned 1'));
      expect(await sync.localIdOf('notes', '100'), isNull);
      expect(await store.getMeta('cursor:notes'), isNull);
    });

    test('a record sent twice in one page is stored once', () async {
      final batches = <List<String>>[];
      final sync = engine([
        SyncEntityConfig(
          name: 'notes',
          remote: _pages({
            null: () => const PullPage(records: [
                  {'id': 1, 'text': 'first'},
                  {'id': 2, 'text': 'other'},
                  {'id': 1, 'text': 'second'},
                  {'id': 3, 'text': 'third'},
                ], nextCursor: 'c1'),
          }),
          local: batching('notes', batches),
        ),
      ]);
      final result = await sync.pullNow();
      expect(result.pulled, 4);
      expect(db.table('notes'), hasLength(3));
      expect(db.table('notes')['local-1']!['text'], 'second');
      expect(batches, [
        ['1', '2'],
        ['1', '3'],
      ]);
      expect(await sync.localIdOf('notes', '1'), 'local-1');
    });

    test('a record can reference another one of its own page', () async {
      final batches = <List<String>>[];
      final sync = engine([
        SyncEntityConfig(
          name: 'folders',
          remote: _pages({
            null: () => const PullPage(records: [
                  {'id': 1, 'name': 'Root'},
                  {'id': 2, 'name': 'Docs', 'parentId': 1},
                  {'id': 3, 'name': 'Work', 'parentId': 2},
                  {'id': 4, 'name': 'Play', 'parentId': 2},
                  {'id': 5, 'name': 'Lost', 'parentId': 77},
                ], nextCursor: 'c1'),
          }),
          local: batching('folders', batches),
          references: [SyncReference('parentId', 'folders')],
        ),
      ]);
      final result = await sync.pullNow();
      expect(result.pulled, 5);
      final folders = db.table('folders');
      expect(folders['local-1']!.containsKey('parentId'), isFalse);
      expect(folders['local-2']!['parentId'], 'local-1');
      expect(folders['local-3']!['parentId'], 'local-2');
      expect(folders['local-4']!['parentId'], 'local-2');
      // Unknown on this device: left as the server sent it.
      expect(folders['local-5']!['parentId'], 77);
      // Only as many writes as the references need.
      expect(batches, [
        ['1'],
        ['2'],
        ['3', '4', '5'],
      ]);
    });

    test('the records of the adapter are not changed', () async {
      final records = [
        {
          'id': 5,
          'customerId': 1,
          'lines': [
            {'itemId': 1},
          ],
        },
      ];
      final sync = engine([
        SyncEntityConfig(
          name: 'bills',
          remote: _pages({
            null: () => PullPage(records: records, nextCursor: 'c1'),
          }),
          local: db.adapter('bills'),
          references: [
            SyncReference('customerId', 'customers'),
            SyncReference('lines[].itemId', 'customers'),
          ],
        ),
        entity('customers', server, db),
      ]);
      await sync.registerMapping('customers', 'c-local', '1');
      await sync.pullNow();
      expect(db.table('bills').values.single, {
        'id': 5,
        'customerId': 'c-local',
        'lines': [
          {'itemId': 'c-local'},
        ],
      });
      expect(records.single['customerId'], 1);
      expect(records.single['lines'], [
        {'itemId': 1},
      ]);
    });

    test('conflicts and deletes keep their place in the page', () async {
      final batches = <List<String>>[];
      final resolved = <String>[];
      final sync = engine([
        SyncEntityConfig(
          name: 'notes',
          remote: _pages({
            null: () => const PullPage(
                  records: [
                    {'id': 1, 'text': 'one'},
                    {'id': 2, 'text': 'server two'},
                    {'id': 3, 'text': 'three'},
                    {'id': 4, 'text': 'server four'},
                    {'id': 5, 'text': 'five'},
                    {'id': 1, 'deleted': true},
                    {'id': 6, 'text': 'six'},
                  ],
                  deletedServerIds: ['5', '99'],
                  nextCursor: 'c1',
                ),
          }),
          local: batching('notes', batches),
          isDeletedOf: (record) => record['deleted'] == true,
          conflictResolver: (conflict) {
            resolved.add(conflict.serverId!);
            return conflict.serverId == '2'
                ? const ConflictResolution.keepLocal()
                : const ConflictResolution.takeServer();
          },
        ),
      ]);
      db.table('notes')['two'] = {'text': 'old two'};
      db.table('notes')['four'] = {'text': 'old four'};
      await sync.registerMappings('notes', {'two': '2', 'four': '4'});
      await sync.recordUpdate('notes', 'two', {'text': 'local two'});
      await sync.recordUpdate('notes', 'four', {'text': 'local four'});
      final deletedByServer = <String?>[];
      sync.events
          .where((e) => e.type == SyncEventType.recordDeletedByServer)
          .listen((e) => deletedByServer.add(e.message));

      final result = await sync.pullNow();

      expect(result.errors, isEmpty);
      expect(resolved, ['2', '4']);
      // 1, 3, 4, 5 and 6 were written; 2 kept its local change.
      expect(result.pulled, 5);
      expect(batches, [
        ['1'],
        ['3'],
        ['4', '5'],
        ['6'],
      ]);
      final notes = db.table('notes');
      expect(
          notes.keys, unorderedEquals(['two', 'local-3', 'four', 'local-6']));
      expect(notes['two']!['text'], 'old two');
      expect(notes['four']!['text'], 'server four');
      expect((await sync.operationsOf('notes', 'two')).single.force, isTrue);
      expect(await sync.operationsOf('notes', 'four'), isEmpty);
      await settle();
      expect(deletedByServer, ['1', '5']);
    });

    test('several of its own records are recognized in one page', () async {
      final sync = engine([
        SyncEntityConfig(
          name: 'notes',
          remote: _pages({
            null: () => const PullPage(records: [
                  {'id': 1, 'clientId': 'a', 'text': 'A'},
                  {'id': 2, 'clientId': 'zzz', 'text': 'someone else'},
                  {'id': 3, 'clientId': 'b', 'text': 'B'},
                  {'id': 4, 'clientId': 'a', 'text': 'a copy'},
                ], nextCursor: 'c1'),
          }),
          local: db.adapter('notes'),
          clientIdOf: (record) => record['clientId'],
        ),
      ]);
      await sync.recordCreate('notes', 'a', {'clientId': 'a', 'text': 'A'});
      await sync.recordCreate('notes', 'b', {'clientId': 'b', 'text': 'B'});
      final result = await sync.pullNow();

      expect(result.errors, isEmpty);
      expect(await sync.serverIdOf('notes', 'a'), '1');
      expect(await sync.serverIdOf('notes', 'b'), '3');
      expect(db.serverIdsAssigned, ['notes/a=1', 'notes/b=3']);
      // Their own two are linked (the unpushed local copy wins); the two
      // that belong to nobody here are stored as new rows.
      expect(db.table('notes'), hasLength(2));
      expect(await sync.localIdOf('notes', '2'), isNotNull);
      expect(await sync.localIdOf('notes', '4'), isNotNull);
      expect(await sync.localIdOf('notes', '4'), isNot('a'));
    });
  });
}
