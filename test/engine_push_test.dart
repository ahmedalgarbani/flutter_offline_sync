import 'dart:async';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

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

  group('recording', () {
    test('a create is pushed and mapped to its server id', () async {
      final sync = engine([entity('notes', server, db)]);
      final events = <SyncEventType>[];
      sync.events.listen((e) => events.add(e.type));

      await sync.recordCreate('notes', 'a', {'text': 'hello'});
      expect(await sync.recordState('notes', 'a'), RecordSyncState.pending);
      final result = await sync.syncNow();

      final serverId = await sync.serverIdOf('notes', 'a');
      expect(result.pushed, 1);
      expect(result.isSuccess, isTrue);
      expect(server.table('notes')[serverId]?['text'], 'hello');
      expect(await sync.localIdOf('notes', serverId!), 'a');
      expect(db.serverIdsAssigned, ['notes/a=$serverId']);
      expect(db.pushed, ['create notes/a']);
      expect(await sync.recordState('notes', 'a'), RecordSyncState.synced);
      expect(sync.status.value.isUpToDate, isTrue);
      expect(sync.status.value.lastSyncedAt, isNotNull);
      expect(server.pushes.single.idempotencyKey, isNotEmpty);
      await settle();
      expect(
          events,
          containsAllInOrder([
            SyncEventType.operationRecorded,
            SyncEventType.runStarted,
            SyncEventType.operationPushed,
            SyncEventType.runFinished,
          ]));
    });

    test('create then updates collapse into one create', () async {
      final sync = engine([entity('notes', server, db)]);
      await sync.recordCreate('notes', 'a', {'text': 'v1', 'pinned': false});
      await sync.recordUpdate('notes', 'a', {'text': 'v2'});
      await sync.recordUpdate('notes', 'a', {'pinned': true});
      expect(await sync.pendingOperations(), hasLength(1));
      await sync.syncNow();

      expect(server.pushes, hasLength(1));
      expect(server.pushes.single.type, SyncOpType.create);
      expect(server.pushes.single.payload, {'text': 'v2', 'pinned': true});
    });

    test('create then delete sends nothing', () async {
      final sync = engine([entity('notes', server, db)]);
      await sync.recordCreate('notes', 'a', {'text': 'v1'});
      await sync.recordUpdate('notes', 'a', {'text': 'v2'});
      expect(await sync.recordDelete('notes', 'a'), isNull);
      await sync.syncNow();
      expect(server.pushes, isEmpty);
      expect(await sync.pendingOperations(), isEmpty);
    });

    test('deleting a synced record drops its queued updates', () async {
      final sync = engine([entity('notes', server, db)]);
      await sync.recordCreate('notes', 'a', {'text': 'v1'});
      await sync.syncNow();
      await sync.recordUpdate('notes', 'a', {'text': 'v2'});
      await sync.recordDelete('notes', 'a');
      await sync.syncNow();

      expect(server.pushes.map((p) => p.type),
          [SyncOpType.create, SyncOpType.delete]);
      expect(server.table('notes'), isEmpty);
    });

    test('coalesce: false keeps every change', () async {
      final sync = engine([entity('notes', server, db, coalesce: false)]);
      await sync.recordCreate('notes', 'a', {'text': 'v1'});
      await sync.recordUpdate('notes', 'a', {'text': 'v2'});
      await sync.syncNow();
      expect(server.pushes.map((p) => p.type),
          [SyncOpType.create, SyncOpType.update]);
      expect(server.table('notes').values.single['text'], 'v2');
    });

    test('rejects invalid writes', () async {
      final sync = engine([
        entity('notes', server, db),
        SyncEntityConfig(
          name: 'readonly',
          remote: server.adapter('readonly'),
          local: db.adapter('readonly'),
          pushEnabled: false,
        ),
      ]);
      expect(() => sync.recordCreate('unknown', 'a', {}), throwsArgumentError);
      expect(() => sync.recordCreate('notes', '', {}), throwsArgumentError);
      expect(() => sync.recordCreate('readonly', 'a', {}), throwsStateError);
      await sync.registerMapping('notes', 'a', '1');
      await sync.recordDelete('notes', 'a');
      expect(() => sync.recordUpdate('notes', 'a', {}), throwsStateError);
    });

    test('the outbox keeps its own copy of the data', () async {
      final sync = engine([entity('bills', server, db)]);
      final items = [
        {'qty': 1},
      ];
      await sync.recordCreate('bills', 'b', {'items': items});
      items.first['qty'] = 99;
      await sync.syncNow();
      expect(server.pushes.single.payload['items'], [
        {'qty': 1},
      ]);
    });

    test('operationsOf lists the queue of one record', () async {
      final sync = engine([entity('notes', server, db)]);
      server.onPush = (_) => const PushOutcome.rejected('Text is too long');
      await sync.recordCreate('notes', 'a', {'text': 'x' * 500});
      await sync.syncNow();
      final ops = await sync.operationsOf('notes', 'a');
      expect(ops.single.lastError, 'Text is too long');
      expect(await sync.operationsOf('notes', 'other'), isEmpty);
    });
  });

  group('failed records', () {
    test('editing a failed record re-queues it with a fresh budget', () async {
      final sync = engine([entity('notes', server, db)]);
      server.onPush = (r) => r.payload['phone'] == 'bad'
          ? const PushOutcome.rejected('Phone is invalid')
          : null;
      await sync.recordCreate('notes', 'a', {'phone': 'bad'});
      final first = await sync.syncNow();
      expect(first.failed, 1);
      expect(await sync.recordState('notes', 'a'), RecordSyncState.failed);

      await sync.recordUpdate('notes', 'a', {'phone': '0100'});
      final op = (await sync.pendingOperations()).single;
      expect(op.type, SyncOpType.create);
      expect(op.attempts, 0);
      expect(op.lastError, isNull);
      await sync.syncNow();
      expect(await sync.recordState('notes', 'a'), RecordSyncState.synced);
      expect(server.table('notes').values.single['phone'], '0100');
    });

    test('an edit made while a push was in flight is folded in too', () async {
      final gate = Completer<void>();
      var reject = true;
      final sync = engine([entity('notes', server, db)]);
      await sync.registerMapping('notes', 'a', '100');
      server.table('notes')['100'] = {'id': 100};
      server.onPush = (r) async {
        if (!reject) return null;
        await gate.future;
        return const PushOutcome.rejected('bad phone');
      };

      await sync.recordUpdate('notes', 'a', {'phone': 'x'});
      final run = sync.syncNow(); // update #1 goes in flight
      await settle(const Duration(milliseconds: 10));
      await sync.recordUpdate('notes', 'a', {'name': 'n'}); // queued behind
      gate.complete();
      await run; // update #1 rejected, update #2 waits behind it

      reject = false;
      await sync.recordUpdate('notes', 'a', {'phone': '0100'}); // the fix
      expect(await sync.pendingOperations(), hasLength(1));
      await sync.syncNow();
      expect(await sync.recordState('notes', 'a'), RecordSyncState.synced);
      expect(server.pushes.last.payload, {'phone': '0100', 'name': 'n'});
    });

    test('a write during a push becomes a new operation', () async {
      final gate = Completer<void>();
      final sync = engine([entity('notes', server, db)]);
      server.onPush = (r) async {
        if (r.type == SyncOpType.create) await gate.future;
        return null;
      };
      await sync.recordCreate('notes', 'a', {'text': 'v1'});
      final run = sync.syncNow();
      await settle(const Duration(milliseconds: 10));
      await sync.recordUpdate('notes', 'a', {'text': 'v2'});
      gate.complete();
      await run;
      // Not merged into the create being sent: a later pass of the same run
      // sends it as an update.
      expect(server.pushes.map((p) => (p.type, p.payload['text'])), [
        (SyncOpType.create, 'v1'),
        (SyncOpType.update, 'v2'),
      ]);
      expect(server.table('notes').values.single['text'], 'v2');
      expect(await sync.pendingOperations(), isEmpty);
    });

    test('retryFailed re-queues; discard drops', () async {
      final sync = engine([entity('notes', server, db)]);
      var accept = false;
      server.onPush =
          (_) => accept ? null : const PushOutcome.rejected('Server says no');
      await sync.recordCreate('notes', 'a', {'text': '1'});
      await sync.recordCreate('notes', 'b', {'text': '2'});
      await sync.syncNow();
      expect(sync.status.value.failedCount, 2);

      final events = <SyncEvent>[];
      sync.events.listen(events.add);
      final failed = await sync.failedOperations();
      await sync.discard(failed.first.id);
      accept = true;
      await sync.retryFailed();
      await sync.syncNow();

      expect(server.table('notes').values.map((r) => r['text']), ['2']);
      expect(sync.status.value.isUpToDate, isTrue);
      expect(
          events.map((e) => (e.type, e.localId)),
          containsAll([
            (SyncEventType.operationDiscarded, 'a'),
            (SyncEventType.operationRetried, 'b'),
          ]));
    });
  });

  group('references', () {
    List<SyncEntityConfig> shop({
      Object Function(String)? serverIdToJson,
      UnknownReferencePolicy policy = UnknownReferencePolicy.passThrough,
    }) =>
        [
          entity('bills', server, db,
              references: [
                SyncReference('customerId', 'customers'),
                SyncReference('items[].itemId', 'items'),
                SyncReference('payment.accountId', 'accounts'),
              ],
              unknownReferencePolicy: policy),
          entity('customers', server, db,
              references: [SyncReference('accountId', 'accounts')],
              serverIdToJson: serverIdToJson),
          entity('items', server, db, serverIdToJson: serverIdToJson),
          entity('accounts', server, db, serverIdToJson: serverIdToJson),
        ];

    test('parents go first and local ids become server ids', () async {
      final sync = engine(shop());
      // Recorded child first: it waits for its parents within the run.
      await sync.recordCreate('bills', 1, {
        'customerId': 2,
        'items': [
          {'itemId': 3},
          {'itemId': 4},
        ],
        'payment': {'accountId': 5},
      });
      await sync.recordCreate('customers', 2, {'accountId': 5});
      await sync.recordCreate('items', 3, {});
      await sync.recordCreate('items', 4, {});
      await sync.recordCreate('accounts', 5, {});
      final result = await sync.syncNow();

      expect(result.pushed, 5);
      expect(result.waiting, 0);
      Future<int> id(String e, Object l) async =>
          int.parse((await sync.serverIdOf(e, l))!);
      final bill = server.pushes.last;
      expect(bill.entity, 'bills');
      expect(bill.payload, {
        'customerId': await id('customers', 2),
        'items': [
          {'itemId': await id('items', 3)},
          {'itemId': await id('items', 4)},
        ],
        'payment': {'accountId': await id('accounts', 5)},
      });
      final customer = server.pushes.firstWhere((p) => p.entity == 'customers');
      expect(customer.payload['accountId'], await id('accounts', 5));
    });

    test('string local ids can be sent as numeric server ids', () async {
      final plain = engine(shop());
      await plain.recordCreate('customers', 'c-uuid', {});
      await plain.recordCreate('bills', 'b-uuid', {'customerId': 'c-uuid'});
      await plain.syncNow();
      expect(server.pushes.last.payload['customerId'], isA<String>());

      store = InMemorySyncStore();
      server = FakeServer();
      final typed = engine(shop(serverIdToJson: int.parse));
      await typed.recordCreate('customers', 'c-uuid', {});
      await typed.recordCreate('bills', 'b-uuid', {'customerId': 'c-uuid'});
      await typed.syncNow();
      expect(server.pushes.last.payload['customerId'], isA<int>());
    });

    test('a child waits while its parent failed, then follows it', () async {
      final sync = engine(shop());
      var acceptCustomers = false;
      server.onPush = (r) => r.entity == 'customers' && !acceptCustomers
          ? const PushOutcome.rejected('Name is required')
          : null;
      await sync.recordCreate('customers', 'c', {});
      await sync.recordCreate('bills', 'b', {'customerId': 'c'});
      final result = await sync.syncNow();

      expect(result.failed, 1);
      expect(result.waiting, 1);
      final bill = (await sync.operationsOf('bills', 'b')).single;
      expect(bill.isPending, isTrue);
      expect(bill.lastError, contains('customers/c'));
      expect(server.pushes.where((p) => p.entity == 'bills'), isEmpty);

      acceptCustomers = true;
      await sync.recordUpdate('customers', 'c', {'name': 'Ann'});
      await sync.syncNow();
      expect(sync.status.value.isUpToDate, isTrue);
      expect(server.pushes.last.payload['customerId'],
          await sync.serverIdOf('customers', 'c'));
    });

    test('a parent discarded before reaching the server fails its children',
        () async {
      final sync = engine(shop());
      server.onPush = (r) =>
          r.entity == 'customers' ? const PushOutcome.rejected('nope') : null;
      await sync.recordCreate('customers', 'c', {});
      await sync.recordCreate('bills', 'b', {'customerId': 'c'});
      await sync.syncNow();
      await sync.discard((await sync.failedOperations()).single.id);
      await sync.syncNow();

      final failed = (await sync.failedOperations()).single;
      expect(failed.entity, 'bills');
      expect(failed.lastError, contains('deleted or discarded'));
      expect(server.pushes.where((p) => p.entity == 'bills'), isEmpty);
    });

    test('unknown references: pass through, reject, or legacy lookup',
        () async {
      final passing = engine(shop());
      await passing.recordCreate('bills', 'b1', {'customerId': 'srv-9'});
      await passing.syncNow();
      expect(server.pushes.single.payload['customerId'], 'srv-9');

      store = InMemorySyncStore();
      final rejecting = engine(shop(policy: UnknownReferencePolicy.reject));
      await rejecting.recordCreate('bills', 'b1', {'customerId': 'srv-9'});
      await rejecting.syncNow();
      expect((await rejecting.failedOperations()).single.lastError,
          'Unknown reference customers/srv-9');

      store = InMemorySyncStore();
      final legacy = engine([
        entity('bills', server, db,
            references: [SyncReference('customerId', 'customers')],
            unknownReferencePolicy: UnknownReferencePolicy.reject),
        entity('customers', server, db,
            local: CallbackLocalAdapter(
              applyRemote: (r, {localId, required serverId}) async =>
                  localId ?? serverId,
              findServerId: (localId) async => localId == '7' ? '700' : null,
            )),
      ]);
      await legacy.recordCreate('bills', 'b1', {'customerId': 7});
      await legacy.syncNow();
      expect(server.pushes.last.payload['customerId'], 700);
      expect(await legacy.serverIdOf('customers', 7), '700');
    });

    test('records created by another push wait for it', () async {
      late SyncEngine sync;
      final accountsLocal = CallbackLocalAdapter(
        applyRemote: (r, {localId, required serverId}) async =>
            localId ?? serverId,
        onServerIdAssigned: (localId, serverId, {serverRecord}) =>
            sync.registerMapping(
                'profiles', 'p1', serverRecord!['profileId'] as Object),
      );
      sync = engine([
        entity('accounts', server, db, local: accountsLocal),
        entity('profiles', server, db),
        entity('orders', server, db,
            references: [SyncReference('profileId', 'profiles')]),
      ]);
      server.onPush = (r) => r.entity == 'accounts'
          ? const PushOutcome.success(
              serverId: '1', record: {'id': 1, 'profileId': 55})
          : null;

      await sync.recordCreate('orders', 'o1', {'profileId': 'p1'});
      await sync.recordCreate('accounts', 'acc', {'name': 'Ann'});
      await sync.recordCreatedVia('profiles', 'p1',
          viaEntity: 'accounts', viaLocalId: 'acc');
      await sync.syncNow();

      expect(server.pushes.map((p) => p.entity), ['accounts', 'orders']);
      expect(server.pushes.last.payload['profileId'], '55');
      expect(await sync.serverIdOf('profiles', 'p1'), '55');
    });
  });

  group('outcomes', () {
    test('network errors stop the run without counting attempts', () async {
      final sync = engine([entity('notes', server, db)]);
      var offline = true;
      server.onPush =
          (_) => offline ? throw const SyncNetworkException('no route') : null;
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      for (var i = 0; i < 3; i++) {
        final result = await sync.syncNow();
        expect(result.abortedBy, SyncAbortReason.offline);
        expect(result.waiting, 2);
      }
      expect(sync.status.value.phase, SyncPhase.offline);
      expect(sync.status.value.isOnline, isFalse);
      expect(server.pushes, hasLength(3)); // one per run, then it stops
      expect((await sync.pendingOperations()).first.attempts, 0);

      offline = false;
      final result = await sync.syncNow();
      expect(result.pushed, 2);
      expect(sync.status.value.phase, SyncPhase.idle);
      expect(sync.status.value.isOnline, isTrue);
    });

    test('a temporary failure backs off and is retried automatically',
        () async {
      final sync = engine([entity('notes', server, db)],
          config: const SyncConfig(
            syncOnStart: false,
            syncOnResume: false,
            periodicInterval: null,
            debounce: Duration(milliseconds: 10),
          ));
      var calls = 0;
      server.onPush = (_) => ++calls == 1
          ? const PushOutcome.retry('busy',
              retryAfter: Duration(milliseconds: 150))
          : null;
      await sync.start();
      await sync.recordCreate('notes', 'a', {});
      await settle(const Duration(milliseconds: 80));

      expect(calls, 1);
      final op = (await sync.pendingOperations()).single;
      expect(op.attempts, 1);
      expect(op.lastError, 'busy');
      expect(sync.status.value.nextRetryAt, isNotNull);

      await settle(const Duration(milliseconds: 400));
      expect(calls, 2);
      expect(sync.status.value.isUpToDate, isTrue);
      expect(sync.status.value.nextRetryAt, isNull);
    });

    test('a backoff is respected within and across runs', () async {
      var now = DateTime(2024, 1, 1, 12);
      final sync = engine([entity('notes', server, db)],
          config: SyncConfig(
            syncOnStart: false,
            syncOnResume: false,
            autoPushAfterWrite: false,
            periodicInterval: null,
            clock: () => now,
            retryPolicy:
                const RetryPolicy(baseDelay: Duration(seconds: 10), jitter: 0),
          ));
      server.onPush = (_) => const PushOutcome.retry('busy');
      await sync.recordCreate('notes', 'a', {});
      final first = await sync.syncNow();
      expect(first.waiting, 1);
      expect(sync.status.value.nextRetryAt, DateTime(2024, 1, 1, 12, 0, 10));

      await sync.syncNow();
      expect(server.pushes, hasLength(1)); // still backing off

      now = now.add(const Duration(seconds: 11));
      await sync.syncNow();
      expect(server.pushes, hasLength(2));
      expect((await sync.pendingOperations()).single.attempts, 2);
    });

    test('out of attempts, the operation fails and is reported', () async {
      final reported = <SyncOperation>[];
      var now = DateTime(2024);
      final sync = engine([entity('notes', server, db)],
          config: SyncConfig(
            syncOnStart: false,
            syncOnResume: false,
            autoPushAfterWrite: false,
            periodicInterval: null,
            clock: () => now,
            retryPolicy: const RetryPolicy(maxAttempts: 2, jitter: 0),
            onOperationFailed: (op, error) => reported.add(op),
          ));
      server.onPush = (_) => const PushOutcome.retry('down');
      await sync.recordCreate('notes', 'a', {});
      await sync.syncNow();
      now = now.add(const Duration(hours: 1));
      final result = await sync.syncNow();

      expect(result.failed, 1);
      expect(reported.single.status, SyncOpStatus.failed);
      expect(reported.single.attempts, 2);
      expect(sync.status.value.failedCount, 1);
      expect(await sync.recordState('notes', 'a'), RecordSyncState.failed);
    });

    test('a rejection fails one record; the others go on', () async {
      final sync = engine([entity('notes', server, db)]);
      server.onPush = (r) =>
          r.localId == 'a' ? const PushOutcome.rejected('Invalid') : null;
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      await sync.syncNow();
      await sync.recordDelete('notes', 'b');
      final result = await sync.syncNow();

      expect(result.pushed, 1);
      expect(server.pushes.map((p) => '${p.type.name} ${p.localId}'),
          ['create a', 'create b', 'delete b']);
      expect(sync.status.value.lastError, isNull);
      expect(sync.status.value.failedCount, 1);
    });

    test('unauthorized stops the run until the next sign-in', () async {
      final sync = engine([entity('notes', server, db)]);
      var signedIn = false;
      server.onPush =
          (_) => signedIn ? null : const PushOutcome.unauthorized('expired');
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      final result = await sync.syncNow();

      expect(result.abortedBy, SyncAbortReason.unauthorized);
      expect(server.pushes, hasLength(1));
      expect(sync.status.value.phase, SyncPhase.authRequired);
      expect((await sync.pendingOperations()).first.attempts, 0);

      signedIn = true;
      await sync.syncNow();
      expect(sync.status.value.phase, SyncPhase.idle);
      expect(sync.status.value.isUpToDate, isTrue);
    });

    test('a create accepted without an id fails instead of duplicating',
        () async {
      final sync = engine([entity('notes', server, db)]);
      server.onPush = (_) => const PushOutcome.success();
      await sync.recordCreate('notes', 'a', {});
      await sync.syncNow();
      await sync.syncNow();
      expect(server.pushes, hasLength(1));
      expect((await sync.failedOperations()).single.lastError,
          contains('returned no id'));
    });

    test('the server id can come from the returned record', () async {
      final sync = engine([entity('notes', server, db)]);
      server.onPush =
          (_) => const PushOutcome.success(record: {'id': 31, 'text': 'x'});
      await sync.recordCreate('notes', 'a', {});
      await sync.syncNow();
      expect(await sync.serverIdOf('notes', 'a'), '31');
    });

    test('a create for a record the server knows is sent as an update',
        () async {
      final sync = engine([entity('notes', server, db)]);
      server.table('notes')['100'] = {'id': 100};
      await sync.registerMapping('notes', 'a', 100);
      await sync.recordCreate('notes', 'a', {'text': 'again'});
      await sync.syncNow();
      expect(server.pushes.single.type, SyncOpType.update);
      expect(server.pushes.single.serverId, '100');
    });

    test('buildPushPayload sends fresh data; if it throws, the push waits',
        () async {
      var fail = true;
      final sync = engine([
        entity('notes', server, db,
            local: CallbackLocalAdapter(
              applyRemote: (r, {localId, required serverId}) async =>
                  localId ?? serverId,
              buildPushPayload: (op) async {
                if (fail) throw StateError('database is locked');
                return {'text': 'fresh'};
              },
            )),
      ]);
      await sync.recordCreate('notes', 'a', {'text': 'stale'});
      final first = await sync.syncNow();
      final op = (await sync.pendingOperations()).single;
      expect(op.status, SyncOpStatus.pending); // not stuck in flight
      expect(op.attempts, 1);
      expect(op.lastError, contains('database is locked'));
      expect(first.waiting, 1);

      fail = false;
      await store.updateOperation(op.copyWith(clearNextAttemptAt: true));
      await sync.syncNow();
      expect(server.pushes.single.payload, {'text': 'fresh'});
    });

    test('a failing onServerIdAssigned never re-sends the create', () async {
      final local = CallbackLocalAdapter(
        applyRemote: (r, {localId, required serverId}) async =>
            localId ?? serverId,
        onServerIdAssigned: (l, s, {serverRecord}) async =>
            throw StateError('no server_id column'),
      );
      final first = engine([entity('notes', server, db, local: local)]);
      await first.recordCreate('notes', 'a', {});
      final result = await first.syncNow();
      expect(result.pushed, 1);
      expect(result.errors.single, contains('no server_id column'));
      expect(await first.serverIdOf('notes', 'a'), isNotNull);
      await first.dispose();

      final restarted = engine([entity('notes', server, db, local: local)]);
      await restarted.syncNow();
      expect(server.pushes, hasLength(1));
    });

    test('a throwing onOperationFailed or logger does not break sync',
        () async {
      final sync = engine([entity('notes', server, db)],
          config: SyncConfig(
            syncOnStart: false,
            syncOnResume: false,
            autoPushAfterWrite: false,
            periodicInterval: null,
            onOperationFailed: (op, e) => throw StateError('ui gone'),
            logger: (level, message, [error, stack]) =>
                throw StateError('logger broken'),
          ));
      server.onPush =
          (r) => r.localId == 'a' ? const PushOutcome.rejected('no') : null;
      await sync.recordCreate('notes', 'a', {});
      await sync.recordCreate('notes', 'b', {});
      final result = await sync.syncNow();
      expect(result.pushed, 1);
      expect(result.failed, 1);
    });
  });

  group('push conflicts', () {
    Future<SyncEngine> conflicted(ConflictResolver resolver) async {
      final sync = engine([
        entity('notes', server, db, conflictResolver: resolver),
      ]);
      server.table('notes')['100'] = {'id': 100, 'text': 'server'};
      await sync.registerMapping('notes', 'a', '100');
      var conflicts = 0;
      server.onPush = (r) => !r.force && conflicts++ == 0
          ? const PushOutcome.conflict(
              serverRecord: {'id': 100, 'text': 'server', 'tag': 's'})
          : null;
      await sync.recordUpdate('notes', 'a', {'text': 'local'});
      return sync;
    }

    test('keepLocal resends with force in the same run', () async {
      final sync =
          await conflicted((_) => const ConflictResolution.keepLocal());
      final result = await sync.syncNow();
      expect(result.pushed, 1);
      expect(server.pushes.map((p) => p.force), [false, true]);
      expect(server.table('notes')['100']!['text'], 'local');
    });

    test('takeServer drops the local change and applies the server copy',
        () async {
      final sync =
          await conflicted((_) => const ConflictResolution.takeServer());
      await sync.syncNow();
      expect(server.pushes, hasLength(1));
      expect(await sync.pendingOperations(), isEmpty);
      expect(db.table('notes')['a'], {'id': 100, 'text': 'server', 'tag': 's'});
    });

    test('merge applies and resends the merged record', () async {
      final sync = await conflicted((c) =>
          ConflictResolution.merge({...c.serverRecord!, ...c.localPayload}));
      await sync.syncNow();
      expect(server.pushes.last.force, isTrue);
      expect(
          server.pushes.last.payload, {'id': 100, 'text': 'local', 'tag': 's'});
      expect(db.table('notes')['a']!['text'], 'local');
    });

    test('a throwing resolver backs off instead of sticking', () async {
      final sync = await conflicted((_) => throw StateError('resolver bug'));
      await sync.syncNow();
      final op = (await sync.pendingOperations()).single;
      expect(op.status, SyncOpStatus.pending);
      expect(op.lastError, contains('resolver bug'));
    });
  });

  test('works the same on SqlSyncStore', () async {
    final database = await openTestDatabase();
    addTearDown(database.close);
    final sync = SyncEngine(
      store: SqlSyncStore(SqfliteExecutor(database)),
      config: manualConfig,
      entities: [
        entity('customers', server, db),
        entity('bills', server, db,
            references: [SyncReference('customerId', 'customers')]),
      ],
    );
    addTearDown(sync.dispose);
    await sync.recordCreate('bills', 'b', {'customerId': 'c'});
    await sync.recordCreate('customers', 'c', {'name': 'Ann'});
    await sync.recordUpdate('customers', 'c', {'phone': '1'});
    final result = await sync.syncNow();

    expect(result.pushed, 2);
    expect(server.pushes.first.payload, {'name': 'Ann', 'phone': '1'});
    expect(server.pushes.last.payload['customerId'],
        await sync.serverIdOf('customers', 'c'));
    expect(sync.status.value.isUpToDate, isTrue);
  });
}
