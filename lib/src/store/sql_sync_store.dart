import 'dart:async';

import '../core/operation.dart';
import 'sync_store.dart';

/// Minimal SQL access used by [SqlSyncStore]. Implement it on top of the
/// database your app already has (drift, sqflite, sqlite3, ...) so the
/// outbox lives next to your data.
///
/// drift:
/// ```dart
/// class DriftSqlExecutor implements SqlExecutor {
///   DriftSqlExecutor(this.db);
///   final GeneratedDatabase db;
///   @override
///   Future<void> execute(String sql, [List<Object?> args = const []]) =>
///       db.customStatement(sql, args);
///   @override
///   Future<List<Map<String, Object?>>> query(String sql,
///           [List<Object?> args = const []]) async =>
///       (await db.customSelect(sql,
///               variables: [for (final a in args) Variable(a)]).get())
///           .map((row) => row.data)
///           .toList();
///   @override
///   Future<T> transaction<T>(Future<T> Function() action) =>
///       db.transaction(action);
/// }
/// ```
abstract class SqlExecutor {
  Future<void> execute(String sql, [List<Object?> args = const []]);

  Future<List<Map<String, Object?>>> query(String sql,
      [List<Object?> args = const []]);

  /// Runs [action] in a transaction. Statements issued through this executor
  /// inside [action] must belong to that transaction (drift does this
  /// automatically through zones).
  Future<T> transaction<T>(Future<T> Function() action);
}

/// [SyncStore] on any SQLite database through a [SqlExecutor].
///
/// Tables are created with `CREATE TABLE IF NOT EXISTS`, so no migration of
/// your schema is needed. [tablePrefix] avoids clashes with your tables.
class SqlSyncStore implements SyncStore {
  SqlSyncStore(this.executor, {this.tablePrefix = 'sync_'});

  final SqlExecutor executor;
  final String tablePrefix;

  static final Object _txKey = Object();

  String get _outbox => '${tablePrefix}outbox';
  String get _idMap => '${tablePrefix}id_map';
  String get _meta => '${tablePrefix}meta';

  static const _opColumns = 'seq, id, entity, local_id, op_type, payload, '
      'status, attempts, force, created_at, updated_at, next_attempt_at, '
      'last_error';

  @override
  Future<void> init() async {
    await executor.execute('''
      CREATE TABLE IF NOT EXISTS $_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        entity TEXT NOT NULL,
        local_id TEXT NOT NULL,
        op_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        force INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        next_attempt_at INTEGER,
        last_error TEXT
      )''');
    await executor.execute('CREATE INDEX IF NOT EXISTS '
        'idx_${_outbox}_record ON $_outbox (entity, local_id)');
    await executor.execute('CREATE INDEX IF NOT EXISTS '
        'idx_${_outbox}_status ON $_outbox (status)');
    await executor.execute('''
      CREATE TABLE IF NOT EXISTS $_idMap (
        entity TEXT NOT NULL,
        local_id TEXT NOT NULL,
        server_id TEXT,
        PRIMARY KEY (entity, local_id)
      )''');
    await executor.execute('CREATE INDEX IF NOT EXISTS '
        'idx_${_idMap}_server ON $_idMap (entity, server_id)');
    await executor.execute('''
      CREATE TABLE IF NOT EXISTS $_meta (
        key TEXT PRIMARY KEY,
        value TEXT
      )''');
  }

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    if (Zone.current[_txKey] == true) return action();
    return executor.transaction(
        () => runZoned(action, zoneValues: {_txKey: true}));
  }

  @override
  Future<SyncOperation> insertOperation(SyncOperation operation) async {
    final row = operation.toRow();
    final columns = row.keys.join(', ');
    final marks = List.filled(row.length, '?').join(', ');
    await executor.execute(
        'INSERT INTO $_outbox ($columns) VALUES ($marks)', row.values.toList());
    final result = await executor
        .query('SELECT seq FROM $_outbox WHERE id = ?', [operation.id]);
    return operation.copyWith(seq: (result.first['seq']! as num).toInt());
  }

  @override
  Future<void> updateOperation(SyncOperation operation) async {
    final row = operation.toRow()..remove('id');
    final sets = row.keys.map((c) => '$c = ?').join(', ');
    await executor.execute('UPDATE $_outbox SET $sets WHERE id = ?',
        [...row.values, operation.id]);
  }

  @override
  Future<void> deleteOperation(String id) =>
      executor.execute('DELETE FROM $_outbox WHERE id = ?', [id]);

  @override
  Future<SyncOperation?> operationById(String id) async {
    final rows = await executor
        .query('SELECT $_opColumns FROM $_outbox WHERE id = ?', [id]);
    return rows.isEmpty ? null : SyncOperation.fromRow(rows.first);
  }

  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) async {
    final List<Map<String, Object?>> rows;
    if (statuses == null) {
      rows = await executor
          .query('SELECT $_opColumns FROM $_outbox ORDER BY seq ASC');
    } else if (statuses.isEmpty) {
      return [];
    } else {
      final marks = List.filled(statuses.length, '?').join(', ');
      rows = await executor.query(
          'SELECT $_opColumns FROM $_outbox WHERE status IN ($marks) '
          'ORDER BY seq ASC',
          statuses.map((s) => s.name).toList());
    }
    return rows.map(SyncOperation.fromRow).toList();
  }

  @override
  Future<List<SyncOperation>> operationsFor(
      String entity, String localId) async {
    final rows = await executor.query(
        'SELECT $_opColumns FROM $_outbox WHERE entity = ? AND local_id = ? '
        'ORDER BY seq ASC',
        [entity, localId]);
    return rows.map(SyncOperation.fromRow).toList();
  }

  @override
  Future<Map<SyncOpStatus, int>> countByStatus() async {
    final counts = {for (final s in SyncOpStatus.values) s: 0};
    final rows = await executor.query(
        'SELECT status, COUNT(*) AS n FROM $_outbox GROUP BY status');
    for (final row in rows) {
      final status = SyncOpStatus.values.byName(row['status']! as String);
      counts[status] = (row['n']! as num).toInt();
    }
    return counts;
  }

  @override
  Future<void> resetInFlight() => executor.execute(
      'UPDATE $_outbox SET status = ? WHERE status = ?',
      [SyncOpStatus.pending.name, SyncOpStatus.inFlight.name]);

  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) async {
    final rows = await executor.query(
        'SELECT server_id FROM $_idMap WHERE entity = ? AND local_id = ?',
        [entity, localId]);
    if (rows.isEmpty) return null;
    return IdMapping(
      entity: entity,
      localId: localId,
      serverId: rows.first['server_id']?.toString(),
    );
  }

  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) async {
    final rows = await executor.query(
        'SELECT local_id FROM $_idMap WHERE entity = ? AND server_id = ? '
        'LIMIT 1',
        [entity, serverId]);
    if (rows.isEmpty) return null;
    return IdMapping(
      entity: entity,
      localId: rows.first['local_id']!.toString(),
      serverId: serverId,
    );
  }

  @override
  Future<void> putMapping(String entity, String localId, String? serverId) =>
      executor.execute(
          'INSERT OR REPLACE INTO $_idMap (entity, local_id, server_id) '
          'VALUES (?, ?, ?)',
          [entity, localId, serverId]);

  @override
  Future<void> deleteMapping(String entity, String localId) =>
      executor.execute('DELETE FROM $_idMap WHERE entity = ? AND local_id = ?',
          [entity, localId]);

  @override
  Future<String?> getMeta(String key) async {
    final rows =
        await executor.query('SELECT value FROM $_meta WHERE key = ?', [key]);
    return rows.isEmpty ? null : rows.first['value'] as String?;
  }

  @override
  Future<void> setMeta(String key, String? value) {
    if (value == null) {
      return executor.execute('DELETE FROM $_meta WHERE key = ?', [key]);
    }
    return executor.execute(
        'INSERT OR REPLACE INTO $_meta (key, value) VALUES (?, ?)',
        [key, value]);
  }

  @override
  Future<void> clear() async {
    await executor.execute('DELETE FROM $_outbox');
    await executor.execute('DELETE FROM $_idMap');
    await executor.execute('DELETE FROM $_meta');
  }
}
