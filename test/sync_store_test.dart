import 'dart:async';

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
    );

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

    test('clear removes operations, mappings and meta', () async {
      await store.insertOperation(_op('1'));
      await store.putMapping('notes', 'a', '1');
      await store.setMeta('k', 'v');
      await store.clear();
      expect(await store.operations(), isEmpty);
      expect(await store.mappingForLocal('notes', 'a'), isNull);
      expect(await store.getMeta('k'), isNull);
    });
  });
}

void main() {
  storeContract('InMemorySyncStore', () async => InMemorySyncStore());

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
  });

  group('SqlSyncStore', () {
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
