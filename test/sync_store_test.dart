import 'dart:async';
import 'dart:convert';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import 'support/fakes.dart';

SyncOperation _op(
  String id, {
  String entity = 'notes',
  String localId = 'a',
  SyncOpType type = SyncOpType.create,
  Map<String, dynamic> payload = const {'text': 'x'},
  SyncOpStatus status = SyncOpStatus.pending,
  DateTime? nextAttemptAt,
}) =>
    SyncOperation(
      id: id,
      entity: entity,
      localId: localId,
      type: type,
      payload: payload,
      status: status,
      createdAt: DateTime(2024, 1, 1),
      updatedAt: DateTime(2024, 1, 2),
      nextAttemptAt: nextAttemptAt,
    );

/// A store with only the required members of [SyncStore], to exercise the
/// paged and batched members it inherits.
class _MinimalStore extends SyncStore {
  final InMemorySyncStore _inner = InMemorySyncStore();

  @override
  Future<void> init() => _inner.init();
  @override
  Future<T> transaction<T>(Future<T> Function() action) =>
      _inner.transaction(action);
  @override
  Future<SyncOperation> insertOperation(SyncOperation operation) =>
      _inner.insertOperation(operation);
  @override
  Future<void> updateOperation(SyncOperation operation) =>
      _inner.updateOperation(operation);
  @override
  Future<void> deleteOperation(String id) => _inner.deleteOperation(id);
  @override
  Future<SyncOperation?> operationById(String id) => _inner.operationById(id);
  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) =>
      _inner.operations(statuses: statuses);
  @override
  Future<List<SyncOperation>> operationsFor(String entity, String localId) =>
      _inner.operationsFor(entity, localId);
  @override
  Future<Map<SyncOpStatus, int>> countByStatus() => _inner.countByStatus();
  @override
  Future<void> resetInFlight() => _inner.resetInFlight();
  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) =>
      _inner.mappingForLocal(entity, localId);
  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) =>
      _inner.mappingForServer(entity, serverId);
  @override
  Future<void> putMapping(String entity, String localId, String? serverId) =>
      _inner.putMapping(entity, localId, serverId);
  @override
  Future<void> deleteMapping(String entity, String localId) =>
      _inner.deleteMapping(entity, localId);
  @override
  Future<String?> getMeta(String key) => _inner.getMeta(key);
  @override
  Future<void> setMeta(String key, String? value) => _inner.setMeta(key, value);
  @override
  Future<void> clear() => _inner.clear();
}

/// Behaviour every [SyncStore] must share.
void storeContract(String name, Future<SyncStore> Function() open) {
  group(name, () {
    late SyncStore store;

    setUp(() async {
      store = await open();
      await store.init();
    });

    test('init can run again on an existing store', () async {
      await store.insertOperation(_op('1'));
      await store.init();
      expect(await store.operations(), hasLength(1));
    });

    test('assigns increasing seq and lists operations in that order', () async {
      final a = await store.insertOperation(_op('1', localId: 'a'));
      final b = await store.insertOperation(_op('2', localId: 'b'));
      final c = await store.insertOperation(_op('3', localId: 'a'));
      expect(a.seq, lessThan(b.seq!));
      expect(b.seq, lessThan(c.seq!));
      expect((await store.operations()).map((o) => o.id), ['1', '2', '3']);
      expect((await store.operationsFor('notes', 'a')).map((o) => o.id),
          ['1', '3']);
      expect(await store.operationsFor('other', 'a'), isEmpty);
    });

    test('round-trips every field and JSON payloads', () async {
      final payload = {
        'text': 'héllo',
        'n': 3,
        'd': 1.5,
        'flag': true,
        'none': null,
        'items': [
          {
            'id': 1,
            'tags': ['a', 'b']
          },
        ],
        'nested': {'x': 'y'},
      };
      final inserted = await store.insertOperation(SyncOperation(
        id: 'op',
        entity: 'bills',
        localId: '7',
        type: SyncOpType.update,
        payload: payload,
        status: SyncOpStatus.failed,
        attempts: 3,
        force: true,
        createdAt: DateTime(2024, 1, 1, 10),
        updatedAt: DateTime(2024, 1, 1, 11),
        nextAttemptAt: DateTime(2024, 1, 1, 12),
        lastError: 'boom',
      ));
      final read = (await store.operationById('op'))!;
      expect(read.seq, inserted.seq);
      expect(read.entity, 'bills');
      expect(read.localId, '7');
      expect(read.type, SyncOpType.update);
      expect(read.payload, payload);
      expect(read.status, SyncOpStatus.failed);
      expect(read.attempts, 3);
      expect(read.force, isTrue);
      expect(read.createdAt, DateTime(2024, 1, 1, 10));
      expect(read.updatedAt, DateTime(2024, 1, 1, 11));
      expect(read.nextAttemptAt, DateTime(2024, 1, 1, 12));
      expect(read.lastError, 'boom');
      expect(await store.operationById('missing'), isNull);
    });

    test('updates keep seq; deletes remove', () async {
      final a = await store.insertOperation(_op('1'));
      await store.updateOperation(a.copyWith(
          payload: {'text': 'changed'}, status: SyncOpStatus.inFlight));
      final read = (await store.operationById('1'))!;
      expect(read.seq, a.seq);
      expect(read.payload, {'text': 'changed'});
      expect(read.status, SyncOpStatus.inFlight);

      await store.deleteOperation('1');
      expect(await store.operationById('1'), isNull);
      // Updating or deleting a missing operation is a no-op.
      await store.updateOperation(a);
      await store.deleteOperation('1');
      expect(await store.operations(), isEmpty);
    });

    test('filters and counts by status', () async {
      await store.insertOperation(_op('1'));
      await store.insertOperation(_op('2', status: SyncOpStatus.failed));
      await store.insertOperation(_op('3', status: SyncOpStatus.inFlight));
      await store.insertOperation(_op('4'));
      expect(
          (await store.operations(statuses: {SyncOpStatus.pending}))
              .map((o) => o.id),
          ['1', '4']);
      expect(
          (await store.operations(
                  statuses: {SyncOpStatus.failed, SyncOpStatus.inFlight}))
              .map((o) => o.id),
          ['2', '3']);
      expect(await store.operations(statuses: {}), isEmpty);
      expect(await store.countByStatus(), {
        SyncOpStatus.pending: 2,
        SyncOpStatus.inFlight: 1,
        SyncOpStatus.failed: 1,
      });
    });

    test('resetInFlight puts interrupted operations back in the queue',
        () async {
      await store.insertOperation(_op('1', status: SyncOpStatus.inFlight));
      await store.insertOperation(_op('2', status: SyncOpStatus.failed));
      await store.resetInFlight();
      expect((await store.operationById('1'))!.status, SyncOpStatus.pending);
      expect((await store.operationById('2'))!.status, SyncOpStatus.failed);
    });

    test('maps ids both ways, including local-only records', () async {
      await store.putMapping('notes', 'a', null);
      final localOnly = (await store.mappingForLocal('notes', 'a'))!;
      expect(localOnly.isLocalOnly, isTrue);

      await store.putMapping('notes', 'a', '100');
      await store.putMapping('bills', 'a', '200');
      expect((await store.mappingForLocal('notes', 'a'))!.serverId, '100');
      expect((await store.mappingForServer('notes', '100'))!.localId, 'a');
      expect(await store.mappingForServer('notes', '200'), isNull);
      expect(await store.mappingForLocal('notes', 'b'), isNull);

      await store.deleteMapping('notes', 'a');
      expect(await store.mappingForLocal('notes', 'a'), isNull);
      expect((await store.mappingForLocal('bills', 'a'))!.serverId, '200');
    });

    test('stores and removes meta values', () async {
      expect(await store.getMeta('cursor'), isNull);
      await store.setMeta('cursor', '1');
      await store.setMeta('cursor', '2');
      expect(await store.getMeta('cursor'), '2');
      await store.setMeta('cursor', null);
      expect(await store.getMeta('cursor'), isNull);
    });

    test('a failed transaction leaves nothing behind', () async {
      await store.insertOperation(_op('kept'));
      await expectLater(
        store.transaction(() async {
          await store.insertOperation(_op('lost'));
          await store.putMapping('notes', 'x', '1');
          await store.setMeta('k', 'v');
          // Nested transactions join the outer one.
          await store.transaction(() => store.deleteOperation('kept'));
          throw StateError('boom');
        }),
        throwsStateError,
      );
      expect((await store.operations()).map((o) => o.id), ['kept']);
      expect(await store.mappingForLocal('notes', 'x'), isNull);
      expect(await store.getMeta('k'), isNull);
    });

    test('a transaction returns its value and commits', () async {
      final value = await store.transaction(() async {
        await store.setMeta('k', 'v');
        return 42;
      });
      expect(value, 42);
      expect(await store.getMeta('k'), 'v');
    });

    test('rejects payloads a database cannot store', () async {
      await expectLater(
          store.insertOperation(_op('1', payload: {'at': DateTime(2024)})),
          throwsA(isA<JsonUnsupportedObjectError>()));
      expect(await store.operations(), isEmpty);
    });

    test('clear removes operations, mappings and meta', () async {
      await store.insertOperation(_op('1'));
      await store.putMapping('notes', 'a', '1');
      await store.setMeta('k', 'v');
      await store.clear();
      expect(await store.operations(), isEmpty);
      expect(await store.mappingForLocal('notes', 'a'), isNull);
      expect(await store.getMeta('k'), isNull);
      expect(await store.countByStatus(), {
        SyncOpStatus.pending: 0,
        SyncOpStatus.inFlight: 0,
        SyncOpStatus.failed: 0,
      });
    });

    group('pages and batches', () {
      final now = DateTime(2024, 6, 1, 12);
      final later = now.add(const Duration(minutes: 5));
      final earlier = now.subtract(const Duration(minutes: 5));

      /// Ten operations: odd ones are bills, 4 failed, 7 in flight, 6 backing
      /// off until [later], 8 due since [earlier].
      Future<void> fill() async {
        for (var i = 1; i <= 10; i++) {
          await store.insertOperation(_op(
            '$i',
            entity: i.isOdd ? 'bills' : 'notes',
            localId: 'r$i',
            status: switch (i) {
              4 => SyncOpStatus.failed,
              7 => SyncOpStatus.inFlight,
              _ => SyncOpStatus.pending,
            },
            nextAttemptAt: switch (i) {
              6 => later,
              8 => earlier,
              _ => null,
            },
          ));
        }
      }

      Future<List<String>> page({
        Set<SyncOpStatus>? statuses,
        Set<String>? entities,
        DateTime? dueAt,
        int afterSeq = 0,
        int limit = 100,
      }) async =>
          [
            for (final op in await store.operationsPage(
                statuses: statuses,
                entities: entities,
                dueAt: dueAt,
                afterSeq: afterSeq,
                limit: limit))
              op.id,
          ];

      test('operationsPage walks the outbox in order, a page at a time',
          () async {
        await fill();
        final first = await store.operationsPage(limit: 4);
        expect(first.map((o) => o.id), ['1', '2', '3', '4']);
        final second =
            await store.operationsPage(afterSeq: first.last.seq!, limit: 4);
        expect(second.map((o) => o.id), ['5', '6', '7', '8']);
        final third =
            await store.operationsPage(afterSeq: second.last.seq!, limit: 4);
        expect(third.map((o) => o.id), ['9', '10']);
        expect(await store.operationsPage(afterSeq: third.last.seq!, limit: 4),
            isEmpty);
        expect(await store.operationsPage(limit: 0), isEmpty);
        // Pages hold whole operations.
        expect(first.first.payload, {'text': 'x'});
        expect(second[1].nextAttemptAt, later);
      });

      test('operationsPage filters by status, entity and due time', () async {
        await fill();
        expect(await page(statuses: {SyncOpStatus.failed}), ['4']);
        expect(
            await page(statuses: {SyncOpStatus.failed, SyncOpStatus.inFlight}),
            ['4', '7']);
        expect(await page(statuses: {}), isEmpty);
        expect(await page(entities: {'bills'}), ['1', '3', '5', '7', '9']);
        expect(await page(entities: {'bills', 'notes'}), hasLength(10));
        expect(await page(entities: {}), isEmpty);
        expect(await page(entities: {'other'}), isEmpty);
        // Operation 6 waits until later; 8 has been due for a while.
        expect(await page(statuses: {SyncOpStatus.pending}, dueAt: now),
            ['1', '2', '3', '5', '8', '9', '10']);
        expect(await page(statuses: {SyncOpStatus.pending}, dueAt: later),
            ['1', '2', '3', '5', '6', '8', '9', '10']);
        expect(
            await page(
                statuses: {SyncOpStatus.pending},
                entities: {'notes'},
                dueAt: now,
                afterSeq: 2,
                limit: 2),
            ['8', '10']);
      });

      test('countOperations counts by status and entity', () async {
        await fill();
        expect(await store.countOperations(), 10);
        expect(
            await store.countOperations(statuses: {SyncOpStatus.pending}), 8);
        expect(
            await store.countOperations(
                statuses: {SyncOpStatus.failed, SyncOpStatus.inFlight}),
            2);
        expect(await store.countOperations(entities: {'bills'}), 5);
        expect(
            await store.countOperations(
                statuses: {SyncOpStatus.pending}, entities: {'bills'}),
            4);
        expect(await store.countOperations(statuses: {}), 0);
        expect(await store.countOperations(entities: {}), 0);
        expect(await store.countOperations(entities: {'other'}), 0);
      });

      test('nextAttemptAfter finds the next pending operation that is due',
          () async {
        expect(await store.nextAttemptAfter(now), isNull);
        await fill();
        expect(await store.nextAttemptAfter(now), later);
        expect(await store.nextAttemptAfter(earlier), later);
        expect(
            await store
                .nextAttemptAfter(earlier.subtract(const Duration(hours: 1))),
            earlier);
        expect(await store.nextAttemptAfter(later), isNull);
        // Only pending operations wait for a retry.
        final six = (await store.operationById('6'))!;
        await store.updateOperation(six.copyWith(status: SyncOpStatus.failed));
        expect(await store.nextAttemptAfter(now), isNull);
      });

      test('clearBackoff makes waiting operations due, keeping the rest',
          () async {
        await fill();
        final five = (await store.operationById('5'))!;
        await store
            .updateOperation(five.copyWith(nextAttemptAt: later, attempts: 3));

        await store.clearBackoff(now, entities: {'notes'});
        expect((await store.operationById('6'))!.nextAttemptAt, isNull);
        expect((await store.operationById('5'))!.nextAttemptAt, later);
        // A time already past is left as it is.
        expect((await store.operationById('8'))!.nextAttemptAt, earlier);

        await store.clearBackoff(now);
        final cleared = (await store.operationById('5'))!;
        expect(cleared.nextAttemptAt, isNull);
        expect(cleared.attempts, 3);
        expect(cleared.payload, {'text': 'x'});
        await store.clearBackoff(now, entities: {});
      });

      test('operationsForRecords reads several records in outbox order',
          () async {
        await store.insertOperation(_op('1', localId: 'a'));
        await store.insertOperation(_op('2', localId: 'b'));
        await store.insertOperation(_op('3', localId: 'a'));
        await store.insertOperation(_op('4', localId: 'c'));
        await store.insertOperation(_op('5', entity: 'bills', localId: 'a'));
        expect(
            (await store.operationsForRecords('notes', ['c', 'a', 'a', 'x']))
                .map((o) => o.id),
            ['1', '3', '4']);
        expect(await store.operationsForRecords('notes', []), isEmpty);
        expect(await store.operationsForRecords('other', ['a']), isEmpty);
      });

      test('markOperations changes only the status', () async {
        final inserted = await store.insertOperation(SyncOperation(
          id: '1',
          entity: 'notes',
          localId: 'a',
          type: SyncOpType.update,
          payload: {'text': 'kept'},
          attempts: 2,
          force: true,
          createdAt: DateTime(2024, 1, 1),
          updatedAt: DateTime(2024, 1, 2),
          nextAttemptAt: later,
          lastError: 'why',
        ));
        await store.insertOperation(_op('2'));
        await store.insertOperation(_op('3'));
        await store
            .markOperations(['1', '3', 'missing'], SyncOpStatus.inFlight);
        final read = (await store.operationById('1'))!;
        expect(read.status, SyncOpStatus.inFlight);
        expect(read.seq, inserted.seq);
        expect(read.payload, {'text': 'kept'});
        expect(read.attempts, 2);
        expect(read.force, isTrue);
        expect(read.nextAttemptAt, later);
        expect(read.lastError, 'why');
        expect((await store.operationById('2'))!.status, SyncOpStatus.pending);
        expect(await store.countByStatus(), {
          SyncOpStatus.pending: 1,
          SyncOpStatus.inFlight: 2,
          SyncOpStatus.failed: 0,
        });
        await store.markOperations([], SyncOpStatus.failed);
      });

      test('deleteOperations removes many at once', () async {
        await fill();
        await store.deleteOperations(['2', '4', '9', 'missing']);
        expect((await store.operations()).map((o) => o.id),
            ['1', '3', '5', '6', '7', '8', '10']);
        expect(await store.operationsFor('notes', 'r2'), isEmpty);
        await store.deleteOperations([]);
        expect(await store.countOperations(), 7);
      });

      test('localIdsWithOperations tells which records have changes', () async {
        await fill();
        expect(
            await store.localIdsWithOperations(
                'notes', ['r2', 'r4', 'r3', 'nope', 'r2']),
            {'r2', 'r4'});
        expect(await store.localIdsWithOperations('notes', []), isEmpty);
        expect(await store.localIdsWithOperations('other', ['r2']), isEmpty);
      });

      test('reads and writes id mappings in batches', () async {
        await store.putMapping('notes', 'old', '1');
        await store.putMappings('notes', {
          'a': '100',
          'b': '200',
          'draft': null,
          'old': '300',
        });
        await store.putMappings('bills', {'a': '100'});
        await store.putMappings('notes', {});

        expect(
            await store
                .localIdsForServer('notes', ['200', '100', '1', '999', '100']),
            {'100': 'a', '200': 'b'});
        expect((await store.mappingForServer('notes', '300'))!.localId, 'old');
        expect(await store.localIdsForServer('notes', []), isEmpty);
        expect(await store.localIdsForServer('other', ['100']), isEmpty);

        final mappings =
            await store.mappingsForLocal('notes', ['a', 'draft', 'missing']);
        expect(mappings.keys, unorderedEquals(['a', 'draft']));
        expect(mappings['a']!.serverId, '100');
        expect(mappings['a']!.entity, 'notes');
        expect(mappings['draft']!.isLocalOnly, isTrue);
        expect(await store.mappingsForLocal('notes', []), isEmpty);
      });

      test('batches larger than one statement can hold', () async {
        const count = 2500;
        final ids = [for (var i = 0; i < count; i++) 'id-$i'];
        await store.transaction(() async {
          for (final id in ids) {
            await store.insertOperation(_op(id, localId: id));
          }
        });
        await store.putMappings('notes', {for (final id in ids) id: 's-$id'});

        final local = await store
            .localIdsForServer('notes', [for (final id in ids) 's-$id']);
        expect(local, hasLength(count));
        expect(local['s-id-2499'], 'id-2499');
        expect(await store.mappingsForLocal('notes', ids), hasLength(count));
        expect(
            await store.localIdsWithOperations('notes', ids), hasLength(count));
        expect(
            await store.operationsForRecords('notes', ids), hasLength(count));
        final ordered = await store.operationsForRecords('notes', ids);
        expect(ordered.map((o) => o.id), ids);

        await store.markOperations(ids, SyncOpStatus.inFlight);
        expect((await store.countByStatus())[SyncOpStatus.inFlight], count);
        await store.deleteOperations(ids.skip(1));
        expect((await store.operations()).map((o) => o.id), ['id-0']);
      });

      test('a failed transaction undoes batched changes too', () async {
        await fill();
        await store.putMappings('notes', {'a': '1', 'b': null});
        await store.setMeta('cursor', 'c1');
        await expectLater(
          store.transaction(() async {
            await store.markOperations(['1', '2'], SyncOpStatus.failed);
            await store.deleteOperations(['3', '5']);
            await store.clearBackoff(now);
            await store.putMappings('notes', {'a': '2', 'c': '3'});
            await store.deleteMapping('notes', 'b');
            await store.setMeta('cursor', 'c2');
            await store.insertOperation(_op('11', localId: 'r1'));
            throw StateError('boom');
          }),
          throwsStateError,
        );
        expect((await store.operations()).map((o) => o.id),
            [for (var i = 1; i <= 10; i++) '$i']);
        expect(await store.countByStatus(), {
          SyncOpStatus.pending: 8,
          SyncOpStatus.inFlight: 1,
          SyncOpStatus.failed: 1,
        });
        expect((await store.operationById('6'))!.nextAttemptAt, later);
        expect((await store.operationsFor('bills', 'r3')).single.id, '3');
        expect((await store.operationsFor('bills', 'r1')).single.id, '1');
        expect((await store.mappingForLocal('notes', 'a'))!.serverId, '1');
        expect((await store.mappingForServer('notes', '1'))!.localId, 'a');
        expect(await store.mappingForServer('notes', '2'), isNull);
        expect(
            (await store.mappingForLocal('notes', 'b'))!.isLocalOnly, isTrue);
        expect(await store.mappingForLocal('notes', 'c'), isNull);
        expect(await store.getMeta('cursor'), 'c1');
        // The sequence goes on from where it was.
        final next = await store.insertOperation(_op('12'));
        final ten = (await store.operationById('10'))!;
        expect(next.seq, greaterThan(ten.seq!));
      });

      test('a rolled back clear leaves everything in place', () async {
        await fill();
        await store.putMapping('notes', 'a', '1');
        await store.setMeta('k', 'v');
        await expectLater(
          store.transaction(() async {
            await store.clear();
            await store.insertOperation(_op('new'));
            throw StateError('boom');
          }),
          throwsStateError,
        );
        expect(await store.countOperations(), 10);
        expect((await store.operationsPage(limit: 3)).map((o) => o.id),
            ['1', '2', '3']);
        expect((await store.mappingForServer('notes', '1'))!.localId, 'a');
        expect(await store.getMeta('k'), 'v');
        expect(await store.localIdsWithOperations('notes', ['r2']), {'r2'});
      });
    });
  });
}

/// A [KeyValueStorage] in a map that can be told to fail.
class _MapStorage extends KeyValueStorage {
  final Map<String, String> values = {};
  int writes = 0;
  bool failWrites = false;

  @override
  Future<String?> read(String key) async => values[key];

  @override
  Future<void> write(String key, String? value) async {
    if (failWrites) throw StateError('disk full');
    writes++;
    if (value == null) {
      values.remove(key);
    } else {
      values[key] = value;
    }
  }
}

/// The sqflite setup from the [CallbackSqlExecutor] docs.
CallbackSqlExecutor<DatabaseExecutor> _callbackExecutor(Database db) =>
    CallbackSqlExecutor<DatabaseExecutor>(
      connection: db,
      execute: (c, sql, args) => c.execute(sql, args),
      query: (c, sql, args) => c.rawQuery(sql, args),
      transaction: (action) => db.transaction(action),
    );

void main() {
  storeContract('InMemorySyncStore', () async => InMemorySyncStore());
  storeContract(
      'a store with only the required members', () async => _MinimalStore());
  storeContract(
      'KeyValueSyncStore', () async => KeyValueSyncStore(_MapStorage()));

  final databases = <Database>[];
  tearDownAll(() async {
    for (final db in databases) {
      await db.close();
    }
  });
  storeContract('SqlSyncStore (sqlite)', () async {
    final db = await openTestDatabase();
    databases.add(db);
    return SqlSyncStore(SqfliteExecutor(db));
  });
  storeContract('SqlSyncStore (CallbackSqlExecutor)', () async {
    final db = await openTestDatabase();
    databases.add(db);
    return SqlSyncStore(_callbackExecutor(db));
  });

  group('InMemorySyncStore', () {
    test('runs transactions one at a time', () async {
      final store = InMemorySyncStore();
      final order = <String>[];
      final first = store.transaction(() async {
        order.add('first start');
        await Future<void>.delayed(const Duration(milliseconds: 20));
        order.add('first end');
      });
      final second = store.transaction(() async => order.add('second'));
      await Future.wait([first, second]);
      expect(order, ['first start', 'first end', 'second']);
    });

    test('a failed transaction does not undo a later one', () async {
      final store = InMemorySyncStore();
      final failing = store.transaction<void>(() async {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        throw StateError('boom');
      });
      await store.transaction(() => store.setMeta('k', 'v'));
      await expectLater(failing, throwsStateError);
      expect(await store.getMeta('k'), 'v');
    });

    test('calls outside a transaction wait for it', () async {
      final store = InMemorySyncStore();
      final gate = Completer<void>();
      final tx = store.transaction(() async {
        await store.setMeta('k', 'inside');
        await gate.future;
      });
      final read = store.getMeta('k');
      gate.complete();
      await tx;
      expect(await read, 'inside');
    });

    test('keeps its own copy of payloads', () async {
      final store = InMemorySyncStore();
      final items = [
        {'qty': 1},
      ];
      await store.insertOperation(_op('1', payload: {'items': items}));
      items.first['qty'] = 99;
      final read = (await store.operationById('1'))!;
      expect(read.payload['items'], [
        {'qty': 1},
      ]);
    });

    test('hands out copies, so callers cannot change what is stored', () async {
      final store = InMemorySyncStore();
      final inserted = await store.insertOperation(_op('1'));
      inserted.payload['text'] = 'changed';
      (await store.operationById('1'))!.payload['text'] = 'changed';
      (await store.operations()).single.payload['text'] = 'changed';
      (await store.operationsFor('notes', 'a')).single.payload['text'] =
          'changed';
      expect((await store.operationById('1'))!.payload['text'], 'x');
    });
  });

  group('KeyValueSyncStore', () {
    test('survives a restart', () async {
      final storage = _MapStorage();
      final before = KeyValueSyncStore(storage);
      await before.init();
      await before.insertOperation(_op('1', payload: {
        'text': 'héllo',
        'items': [
          {'qty': 2},
        ],
      }));
      await before.insertOperation(_op('2', status: SyncOpStatus.failed));
      await before.putMapping('notes', 'a', '100');
      await before.putMapping('notes', 'b', null);
      await before.setMeta('cursor:notes', '42');

      final after = KeyValueSyncStore(storage);
      await after.init();
      final ops = await after.operations();
      expect(ops.map((o) => o.id), ['1', '2']);
      expect(ops.first.payload, {
        'text': 'héllo',
        'items': [
          {'qty': 2},
        ],
      });
      expect(ops.last.status, SyncOpStatus.failed);
      expect((await after.mappingForLocal('notes', 'a'))!.serverId, '100');
      expect((await after.mappingForLocal('notes', 'b'))!.isLocalOnly, isTrue);
      expect(await after.getMeta('cursor:notes'), '42');
      // Sequence numbers continue where they stopped.
      final next = await after.insertOperation(_op('3'));
      expect(next.seq, greaterThan(ops.last.seq!));
    });

    test('saves a transaction once, when it commits', () async {
      final storage = _MapStorage();
      final store = KeyValueSyncStore(storage);
      await store.init();
      await store.transaction(() async {
        await store.insertOperation(_op('1'));
        await store.putMapping('notes', 'a', null);
        await store.setMeta('k', 'v');
      });
      expect(storage.writes, 1);

      await expectLater(store.transaction<void>(() async {
        await store.setMeta('k', 'rolled back');
        throw StateError('boom');
      }), throwsStateError);
      expect(storage.writes, 1);
      expect(await store.getMeta('k'), 'v');
    });

    test('a change that cannot be saved is rolled back', () async {
      final storage = _MapStorage();
      final store = KeyValueSyncStore(storage);
      await store.init();
      await store.insertOperation(_op('1'));
      storage.failWrites = true;
      await expectLater(store.insertOperation(_op('2')), throwsStateError);
      await expectLater(store.deleteOperation('1'), throwsStateError);
      storage.failWrites = false;
      expect((await store.operations()).map((o) => o.id), ['1']);
    });

    test('unreadable saved state is reported and left untouched', () async {
      final storage = _MapStorage()..values['flutter_offline_first_sync'] = '{';
      final store = KeyValueSyncStore(storage);
      await expectLater(store.init(), throwsFormatException);
      expect(storage.values['flutter_offline_first_sync'], '{');
    });

    test('refuses changes before init, which would lose the saved state',
        () async {
      final storage = _MapStorage();
      final saved = KeyValueSyncStore(storage);
      await saved.init();
      await saved.setMeta('k', 'v');

      final store = KeyValueSyncStore(storage);
      await expectLater(store.setMeta('other', 'x'), throwsStateError);
      await store.init();
      expect(await store.getMeta('k'), 'v');
    });

    test('keeps separate outboxes under separate keys', () async {
      final storage = _MapStorage();
      final ann = KeyValueSyncStore(storage, key: 'sync:ann');
      final bob = KeyValueSyncStore(storage, key: 'sync:bob');
      await ann.init();
      await bob.init();
      await ann.setMeta('k', 'ann');
      await bob.setMeta('k', 'bob');
      final annAgain = KeyValueSyncStore(storage, key: 'sync:ann');
      await annAgain.init();
      expect(await annAgain.getMeta('k'), 'ann');
    });

    test('an engine picks up the outbox after a restart', () async {
      final storage = _MapStorage();
      final server = FakeServer();
      final db = FakeLocalDb();
      SyncEngine open() => SyncEngine(
            store: KeyValueSyncStore(storage),
            config: manualConfig,
            entities: [entity('notes', server, db)],
          );

      final first = open();
      await first.recordCreate('notes', 'a', {'text': 'offline'});
      await first.dispose();

      final second = open();
      addTearDown(second.dispose);
      final result = await second.syncNow();
      expect(result.pushed, 1);
      expect(server.pushes.single.payload, {'text': 'offline'});
      expect(await second.serverIdOf('notes', 'a'), isNotNull);
    });
  });

  group('SqlSyncStore', () {
    test('rejects a table prefix that is not an identifier', () async {
      final db = await openTestDatabase();
      addTearDown(db.close);
      for (final prefix in ['my-app_', 'x y', '1sync_', 'a;DROP']) {
        expect(() => SqlSyncStore(SqfliteExecutor(db), tablePrefix: prefix),
            throwsArgumentError,
            reason: prefix);
      }
      for (final prefix in ['', 'app_sync_', '_s1']) {
        expect(SqlSyncStore(SqfliteExecutor(db), tablePrefix: prefix),
            isA<SqlSyncStore>());
      }
    });

    test('CallbackSqlExecutor commits the app\'s rows and the outbox together',
        () async {
      final db = await openTestDatabase();
      addTearDown(db.close);
      final executor = _callbackExecutor(db);
      final store = SqlSyncStore(executor);
      await store.init();
      await db.execute('CREATE TABLE notes (id TEXT PRIMARY KEY, text TEXT)');

      Future<void> write(String id, {bool fail = false}) =>
          executor.transaction(() async {
            await executor.current.insert('notes', {'id': id, 'text': 'x'});
            // Joins the open transaction instead of waiting for it.
            await store
                .transaction(() => store.insertOperation(_op(id, localId: id)));
            if (fail) throw StateError('boom');
          });

      await write('kept');
      await expectLater(write('dropped', fail: true), throwsStateError);
      final rows = await db.query('notes');
      expect(rows.map((r) => r['id']), ['kept']);
      expect((await store.operations()).map((o) => o.id), ['kept']);
      expect(executor.current, same(db)); // outside a transaction
    });

    test('uses the table prefix', () async {
      final db = await openTestDatabase();
      addTearDown(db.close);
      final store = SqlSyncStore(SqfliteExecutor(db), tablePrefix: 'app_sync_');
      await store.init();
      final tables = await db
          .rawQuery("SELECT name FROM sqlite_master WHERE type = 'table'");
      expect(tables.map((t) => t['name']),
          containsAll(['app_sync_outbox', 'app_sync_id_map', 'app_sync_meta']));
    });

    test('two stores never share a transaction', () async {
      final db1 = await openTestDatabase();
      final db2 = await openTestDatabase();
      addTearDown(db1.close);
      addTearDown(db2.close);
      final one = SqlSyncStore(SqfliteExecutor(db1));
      final two = SqlSyncStore(SqfliteExecutor(db2));
      await one.init();
      await two.init();
      await expectLater(
        one.transaction(() async {
          await two.transaction(() => two.setMeta('k', 'committed'));
          throw StateError('boom');
        }),
        throwsStateError,
      );
      expect(await two.getMeta('k'), 'committed');
    });
  });
}
