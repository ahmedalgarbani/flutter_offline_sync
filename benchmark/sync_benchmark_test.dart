// Throughput benchmark on real SQLite (sqflite_common_ffi), with the outbox
// and the app's table in one database file, as in an app.
//
// Run it with:
//
//   flutter test benchmark/sync_benchmark_test.dart
//
// Sizes can be scaled: `--dart-define=SCALE=10` multiplies every size by ten
// (SCALE=10 pulls a million records).
//
// The remote answers at once, so the numbers show what the engine and the
// database cost, not the network. "stmts/rec" is the number of SQL statements
// per record: on a phone every statement crosses a platform channel, so it
// matters more there than this machine's timings show.
@Timeout(Duration(hours: 2))
library;

import 'dart:io';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

const _scale = int.fromEnvironment('SCALE', defaultValue: 1);

const _config = SyncConfig(
  syncOnStart: false,
  syncOnResume: false,
  syncOnReconnect: false,
  autoPushAfterWrite: false,
  periodicInterval: null,
);

class _Bench {
  _Bench(this.dir, this.db, this.executor);

  final Directory dir;
  final Database db;
  final CallbackSqlExecutor<DatabaseExecutor> executor;

  /// SQL statements sent so far.
  static int statements = 0;

  static Future<_Bench> open() async {
    sqfliteFfiInit();
    final dir = await Directory.systemTemp.createTemp('sync_bench');
    final db = await databaseFactoryFfiNoIsolate.openDatabase(
        '${dir.path}/bench.db',
        options: OpenDatabaseOptions(singleInstance: false));
    await db.execute('CREATE TABLE products ('
        'id TEXT PRIMARY KEY, server_id TEXT, name TEXT, price REAL, '
        'category_id TEXT)');
    await db.execute('CREATE TABLE categories ('
        'id TEXT PRIMARY KEY, server_id TEXT, name TEXT)');
    final executor = CallbackSqlExecutor<DatabaseExecutor>(
      connection: db,
      execute: (c, sql, args) {
        statements++;
        return c.execute(sql, args);
      },
      query: (c, sql, args) {
        statements++;
        return c.rawQuery(sql, args);
      },
      transaction: (action) => db.transaction(action),
    );
    return _Bench(dir, db, executor);
  }

  Future<void> close() async {
    await db.close();
    await dir.delete(recursive: true);
  }

  LocalAdapter local(String table, {bool batch = false}) =>
      batch ? _BatchSqlLocal(executor, table) : _SqlLocal(executor, table);
}

/// Writes a pulled page with one statement per 150 rows.
class _BatchSqlLocal extends _SqlLocal {
  _BatchSqlLocal(super.executor, super.table);

  @override
  Future<List<String>> applyRemoteBatch(List<RemoteChange> changes) async {
    final ids = [for (final c in changes) c.localId ?? 'l${c.serverId}'];
    final products = table == 'products';
    const size = 150;
    for (var start = 0; start < changes.length; start += size) {
      final end = start + size > changes.length ? changes.length : start + size;
      final args = <Object?>[];
      for (var i = start; i < end; i++) {
        final record = changes[i].record;
        args
          ..add(ids[i])
          ..add(changes[i].serverId)
          ..add(record['name']);
        if (products) {
          args
            ..add(record['price'])
            ..add(record['categoryId']?.toString());
        }
      }
      final row = products ? '(?, ?, ?, ?, ?)' : '(?, ?, ?)';
      await executor.execute(
          'INSERT OR REPLACE INTO $table '
          '(id, server_id, name${products ? ', price, category_id' : ''}) '
          'VALUES ${List.filled(end - start, row).join(', ')}',
          args);
    }
    return ids;
  }
}

/// Writes pulled rows one statement at a time.
class _SqlLocal extends LocalAdapter {
  _SqlLocal(this.executor, this.table);

  final CallbackSqlExecutor<DatabaseExecutor> executor;
  final String table;

  @override
  Future<String> applyRemote(Map<String, dynamic> record,
      {required String? localId, required String serverId}) async {
    final id = localId ?? 'l$serverId';
    if (table == 'products') {
      await executor.execute(
          'INSERT OR REPLACE INTO products '
          '(id, server_id, name, price, category_id) VALUES (?, ?, ?, ?, ?)',
          [
            id,
            serverId,
            record['name'],
            record['price'],
            record['categoryId']?.toString(),
          ]);
    } else {
      await executor.execute(
          'INSERT OR REPLACE INTO $table (id, server_id, name) '
          'VALUES (?, ?, ?)',
          [id, serverId, record['name']]);
    }
    return id;
  }

  @override
  Future<void> applyRemoteDelete(String localId, {required String serverId}) =>
      executor.execute('DELETE FROM $table WHERE id = ?', [localId]);
}

/// A server holding [total] products spread over [categories] categories.
RemoteAdapter _catalog(
  int total,
  int categories, {
  List<int>? pushes,
  bool bulk = false,
}) =>
    CallbackRemoteAdapter(
      push: (request) async {
        pushes?.add(1);
        return PushOutcome.success(serverId: 's-${request.localId}');
      },
      pushBatch: !bulk
          ? null
          : (requests) async {
              pushes?.add(requests.length);
              return [
                for (final request in requests)
                  PushOutcome.success(serverId: 's-${request.localId}'),
              ];
            },
      pull: (request) async {
        final offset = int.tryParse(request.cursor ?? '') ?? 0;
        final end =
            offset + request.limit > total ? total : offset + request.limit;
        return PullPage(
          records: [
            for (var i = offset; i < end; i++)
              {
                'id': i + 1,
                'name': 'Product ${i + 1}',
                'price': (i % 1000) / 10,
                'categoryId': (i % categories) + 1,
              },
          ],
          hasMore: end < total,
          nextCursor: '$end',
        );
      },
    );

RemoteAdapter _categories(int total) => CallbackRemoteAdapter(
      push: (request) async =>
          PushOutcome.success(serverId: 's-${request.localId}'),
      pull: (request) async {
        final offset = int.tryParse(request.cursor ?? '') ?? 0;
        final end =
            offset + request.limit > total ? total : offset + request.limit;
        return PullPage(
          records: [
            for (var i = offset; i < end; i++)
              {'id': i + 1, 'name': 'Category ${i + 1}'},
          ],
          hasMore: end < total,
          nextCursor: '$end',
        );
      },
    );

void _report(String name, int count, Duration elapsed, [int? statements]) {
  final perSecond = count * 1000000 / elapsed.inMicroseconds;
  // ignore: avoid_print
  print('BENCH ${name.padRight(46)} ${count.toString().padLeft(8)} in '
      '${(elapsed.inMilliseconds / 1000).toStringAsFixed(2).padLeft(8)} s  '
      '${perSecond.toStringAsFixed(0).padLeft(8)}/s'
      '${statements == null ? '' : '  ${(statements / count).toStringAsFixed(2).padLeft(6)} stmts/rec'}');
}

void main() {
  late _Bench bench;

  setUp(() async => bench = await _Bench.open());
  tearDown(() => bench.close());

  SyncEngine engine({
    required int products,
    int categories = 100,
    List<int>? pushes,
    bool batchWrites = false,
    int pushBatchSize = 1,
    bool bulkEndpoint = false,
  }) {
    final sync = SyncEngine(
      store: SqlSyncStore(bench.executor),
      config: _config,
      entities: [
        SyncEntityConfig(
          name: 'categories',
          remote: _categories(categories),
          local: bench.local('categories', batch: batchWrites),
        ),
        SyncEntityConfig(
          name: 'products',
          remote: _catalog(products, categories,
              pushes: pushes, bulk: bulkEndpoint),
          local: bench.local('products', batch: batchWrites),
          references: [SyncReference('categoryId', 'categories')],
          pushBatchSize: pushBatchSize,
        ),
      ],
    );
    addTearDown(sync.dispose);
    return sync;
  }

  Future<int> rows(String table) async =>
      (await bench.db.rawQuery('SELECT COUNT(*) AS n FROM $table')).first['n']!
          as int;

  for (final batchWrites in [false, true]) {
    final how = batchWrites ? 'applyRemoteBatch' : 'applyRemote';

    test('pull: first sync of a large catalog ($how)', () async {
      const total = 100000 * _scale;
      final sync = engine(products: total, batchWrites: batchWrites);
      var before = _Bench.statements;
      final watch = Stopwatch()..start();
      final result = await sync.pullNow();
      watch.stop();
      expect(result.errors, isEmpty);
      expect(result.pulled, total + 100);
      expect(await rows('products'), total);
      _report('pull, empty outbox ($how)', total, watch.elapsed,
          _Bench.statements - before);
      final row = (await bench.db
              .rawQuery("SELECT * FROM products WHERE server_id = '250'"))
          .single;
      expect(row, {
        'id': 'l250',
        'server_id': '250',
        'name': 'Product 250',
        'price': 24.9,
        'category_id': 'l50',
      });

      // A second pull of the same records updates every row.
      await sync.resetPullCursor('products');
      before = _Bench.statements;
      watch
        ..reset()
        ..start();
      final again = await sync.pullNow(entities: {'products'});
      watch.stop();
      expect(again.pulled, total);
      expect(await rows('products'), total);
      _report('pull again, every row known ($how)', total, watch.elapsed,
          _Bench.statements - before);
    });
  }

  test('pull while the outbox holds unpushed changes', () async {
    const total = 20000 * _scale;
    const queued = 5000 * _scale;
    final sync = engine(products: total);
    await bench.executor.transaction(() async {
      for (var i = 0; i < queued; i++) {
        await sync.recordCreate(
            'categories', 'new-$i', {'name': 'Queued category $i'});
      }
    });
    final before = _Bench.statements;
    final watch = Stopwatch()..start();
    final result = await sync.pullNow(entities: {'products'});
    watch.stop();
    expect(result.errors, isEmpty);
    expect(result.pulled, total);
    _report('pull, $queued changes queued', total, watch.elapsed,
        _Bench.statements - before);
  });

  for (final (name, batchSize, bulk) in [
    ('one at a time', 1, false),
    ('8 side by side', 8, false),
    ('bulk endpoint, 200 per request', 200, true),
  ]) {
    test('record and push a large outbox ($name)', () async {
      const total = 20000 * _scale;
      final pushes = <int>[];
      final sync = engine(
          products: 0,
          pushes: pushes,
          pushBatchSize: batchSize,
          bulkEndpoint: bulk);

      var before = _Bench.statements;
      var watch = Stopwatch()..start();
      await bench.executor.transaction(() async {
        for (var i = 0; i < total; i++) {
          await sync.recordCreate('products', 'p$i', {
            'name': 'Product $i',
            'price': 9.5,
          });
        }
      });
      watch.stop();
      expect((await sync.store.countByStatus())[SyncOpStatus.pending], total);
      if (batchSize == 1) {
        _report('record (one transaction)', total, watch.elapsed,
            _Bench.statements - before);
      }

      before = _Bench.statements;
      watch = Stopwatch()..start();
      final result = await sync.pushNow();
      watch.stop();
      expect(result.pushed, total);
      expect(result.waiting, 0);
      expect(pushes.fold<int>(0, (sum, n) => sum + n), total);
      expect(sync.status.value.pendingCount, 0);
      expect(await sync.serverIdOf('products', 'p7'), 's-p7');
      _report('push, $name', total, watch.elapsed, _Bench.statements - before);
    });
  }

  test('sync with nothing to do, next to a large failed backlog', () async {
    const total = 20000 * _scale;
    final sync = SyncEngine(
      store: SqlSyncStore(bench.executor),
      config: _config,
      entities: [
        SyncEntityConfig(
          name: 'products',
          remote: CallbackRemoteAdapter(
            push: (request) async => const PushOutcome.rejected('invalid'),
          ),
          local: bench.local('products'),
          pullMode: PullMode.none,
        ),
      ],
    );
    addTearDown(sync.dispose);
    await bench.executor.transaction(() async {
      for (var i = 0; i < total; i++) {
        await sync.recordCreate('products', 'p$i', {'name': 'Product $i'});
      }
    });
    await sync.pushNow();
    expect(sync.status.value.failedCount, total);

    const runs = 20;
    final before = _Bench.statements;
    final watch = Stopwatch()..start();
    for (var i = 0; i < runs; i++) {
      await sync.syncNow();
    }
    watch.stop();
    _report('idle sync, $total failed kept', runs, watch.elapsed,
        _Bench.statements - before);
  });
}
