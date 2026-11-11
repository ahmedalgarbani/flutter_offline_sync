import 'dart:async';

import '../core/operation.dart';
import 'sync_store.dart';

/// Minimal SQL access used by [SqlSyncStore]. Implement it on top of the
/// database your app already has (drift, sqflite, sqlite3, ...) so the
/// outbox lives next to your data, or build one from three functions with
/// [CallbackSqlExecutor].
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

/// A [SqlExecutor] built from three functions, for any SQLite API: sqflite,
/// sqlcipher, sqlite3, drift, floor, ...
///
/// [C] is the API's connection type. Inside [transaction], statements are
/// sent to the transaction's connection (routed through a zone), so the
/// outbox and your own writes commit or roll back together. Use [current]
/// for your own statements there:
///
/// ```dart
/// // sqflite
/// final executor = CallbackSqlExecutor<DatabaseExecutor>(
///   connection: db,
///   execute: (c, sql, args) => c.execute(sql, args),
///   query: (c, sql, args) => c.rawQuery(sql, args),
///   transaction: (action) => db.transaction(action),
/// );
/// final sync = SyncEngine(store: SqlSyncStore(executor), ...);
///
/// await executor.transaction(() async {
///   await executor.current.insert('notes', note.toMap());
///   await sync.recordCreate('notes', note.id, note.toJson());
/// });
/// ```
///
/// ```dart
/// // drift: transactions already follow the zone.
/// CallbackSqlExecutor<GeneratedDatabase>(
///   connection: db,
///   execute: (db, sql, args) => db.customStatement(sql, args),
///   query: (db, sql, args) async => [
///     for (final row in await db.customSelect(sql,
///         variables: [for (final a in args) Variable(a)]).get())
///       row.data,
///   ],
///   transaction: (action) => db.transaction(() => action(db)),
/// );
/// ```
///
/// ```dart
/// // sqlite3 (synchronous API)
/// CallbackSqlExecutor<Database>(
///   connection: db,
///   execute: (db, sql, args) async => db.execute(sql, args),
///   query: (db, sql, args) async =>
///       [for (final row in db.select(sql, args)) Map.of(row)],
///   transaction: (action) async {
///     db.execute('BEGIN');
///     try {
///       await action(db);
///       db.execute('COMMIT');
///     } catch (_) {
///       db.execute('ROLLBACK');
///       rethrow;
///     }
///   },
/// );
/// ```
class CallbackSqlExecutor<C> implements SqlExecutor {
  CallbackSqlExecutor({
    required this.connection,
    required Future<void> Function(C connection, String sql, List<Object?> args)
        execute,
    required Future<List<Map<String, Object?>>> Function(
            C connection, String sql, List<Object?> args)
        query,
    required Future<void> Function(Future<void> Function(C transaction) action)
        transaction,
  })  : _execute = execute,
        _query = query,
        _transaction = transaction;

  /// The connection used outside transactions.
  final C connection;

  final Future<void> Function(C connection, String sql, List<Object?> args)
      _execute;
  final Future<List<Map<String, Object?>>> Function(
      C connection, String sql, List<Object?> args) _query;
  final Future<void> Function(Future<void> Function(C transaction) action)
      _transaction;

  /// Per instance, so a transaction of another database is never joined.
  final Object _zoneKey = Object();

  /// The open transaction's connection inside [transaction], [connection]
  /// elsewhere.
  C get current {
    final open = Zone.current[_zoneKey];
    return open is _OpenTransaction<C> ? open.connection : connection;
  }

  @override
  Future<void> execute(String sql, [List<Object?> args = const []]) =>
      _execute(current, sql, args);

  @override
  Future<List<Map<String, Object?>>> query(String sql,
          [List<Object?> args = const []]) =>
      _query(current, sql, args);

  /// Runs [action] in a transaction; nested calls join the open one.
  @override
  Future<T> transaction<T>(Future<T> Function() action) async {
    if (Zone.current[_zoneKey] != null) return action();
    late T result;
    await _transaction((txn) async {
      result = await runZoned(action,
          zoneValues: {_zoneKey: _OpenTransaction<C>(txn)});
    });
    return result;
  }
}

class _OpenTransaction<C> {
  const _OpenTransaction(this.connection);
  final C connection;
}

/// [SyncStore] on any SQLite database through a [SqlExecutor].
///
/// Tables are created with `CREATE TABLE IF NOT EXISTS`, so no migration of
/// your schema is needed. [tablePrefix] avoids clashes with your tables; it
/// may hold letters, digits and underscores.
///
/// Built for large data sets: the outbox is read a page at a time through
/// its indexes, and the id map is read and written a page of records per
/// statement, so cost and memory do not grow with the number of rows.
class SqlSyncStore extends SyncStore {
  SqlSyncStore(this.executor, {this.tablePrefix = 'sync_'}) {
    // The prefix is part of table names written into SQL, never a bound
    // value: anything else would break the statements.
    if (!RegExp(r'^([A-Za-z_][A-Za-z0-9_]*)?$').hasMatch(tablePrefix)) {
      throw ArgumentError.value(tablePrefix, 'tablePrefix',
          'may only hold letters, digits and underscores');
    }
  }

  final SqlExecutor executor;
  final String tablePrefix;

  static final Object _txKey = Object();

  String get _outbox => '${tablePrefix}outbox';
  String get _idMap => '${tablePrefix}id_map';
  String get _meta => '${tablePrefix}meta';

  static const _opColumns = 'seq, id, entity, local_id, op_type, payload, '
      'status, attempts, force, created_at, updated_at, next_attempt_at, '
      'last_error';

  /// Bound values per statement. SQLite before 3.32 (Android 10 and older)
  /// accepts at most 999.
  static const _maxVariables = 900;

  static String _marks(int count) => List.filled(count, '?').join(', ');

  /// Runs [body] for [values] in slices that fit one statement, [reserved]
  /// bound values being used by the rest of the statement.
  static Future<void> _chunked<T>(
    Iterable<T> values,
    Future<void> Function(List<T> chunk) body, {
    int reserved = 1,
    int perValue = 1,
  }) async {
    final size = (_maxVariables - reserved) ~/ perValue;
    var chunk = <T>[];
    for (final value in values) {
      chunk.add(value);
      if (chunk.length == size) {
        await body(chunk);
        chunk = <T>[];
      }
    }
    if (chunk.isNotEmpty) await body(chunk);
  }

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
    // Only operations waiting out a backoff are in this index, so finding
    // the next one that is due does not read the outbox.
    await executor.execute('CREATE INDEX IF NOT EXISTS '
        'idx_${_outbox}_retry ON $_outbox (next_attempt_at) '
        'WHERE next_attempt_at IS NOT NULL');
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
    // Nested calls join the transaction of this store that is already open.
    if (identical(Zone.current[_txKey], this)) return action();
    return executor
        .transaction(() => runZoned(action, zoneValues: {_txKey: this}));
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
    final rows = await executor
        .query('SELECT status, COUNT(*) AS n FROM $_outbox GROUP BY status');
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
  Future<void> deleteMapping(String entity, String localId) => executor.execute(
      'DELETE FROM $_idMap WHERE entity = ? AND local_id = ?',
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
  Future<void> clear() => transaction(() async {
        await executor.execute('DELETE FROM $_outbox');
        await executor.execute('DELETE FROM $_idMap');
        await executor.execute('DELETE FROM $_meta');
      });

  // Paged and batched access --------------------------------------------------

  /// `column = ?` for one value, `column IN (?, ...)` for several.
  static String _among(String column, int count) =>
      count == 1 ? '$column = ?' : '$column IN (${_marks(count)})';

  /// The status condition of a query that returns rows in `seq` order.
  ///
  /// For one status the status index yields the rows already in that order.
  /// For several it would not, and SQLite would sort every matching row to
  /// return one page; the `+` makes it walk the table in `seq` order
  /// instead and stop when the page is full.
  static String _statusInSeqOrder(int count) =>
      count == 1 ? 'status = ?' : '+status IN (${_marks(count)})';

  @override
  Future<List<SyncOperation>> operationsPage({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
    DateTime? dueAt,
    int afterSeq = 0,
    required int limit,
  }) async {
    if (limit <= 0 ||
        (statuses != null && statuses.isEmpty) ||
        (entities != null && entities.isEmpty)) {
      return [];
    }
    final where = <String>['seq > ?'];
    final args = <Object?>[afterSeq];
    if (statuses != null) {
      where.add(_statusInSeqOrder(statuses.length));
      args.addAll(statuses.map((s) => s.name));
    }
    if (entities != null) {
      where.add(_among('entity', entities.length));
      args.addAll(entities);
    }
    if (dueAt != null) {
      where.add('(next_attempt_at IS NULL OR next_attempt_at <= ?)');
      args.add(dueAt.millisecondsSinceEpoch);
    }
    final rows = await executor.query(
        'SELECT $_opColumns FROM $_outbox WHERE ${where.join(' AND ')} '
        'ORDER BY seq ASC LIMIT ?',
        [...args, limit]);
    return rows.map(SyncOperation.fromRow).toList();
  }

  @override
  Future<List<SyncOperation>> operationsForRecords(
      String entity, Iterable<String> localIds) async {
    final all = <SyncOperation>[];
    await _chunked<String>(localIds.toSet(), (chunk) async {
      final rows = await executor.query(
          'SELECT $_opColumns FROM $_outbox '
          'WHERE entity = ? AND local_id IN (${_marks(chunk.length)}) '
          'ORDER BY seq ASC',
          [entity, ...chunk]);
      all.addAll(rows.map(SyncOperation.fromRow));
    });
    // Several statements each return their rows in order; put them together.
    if (all.length > 1) all.sort((a, b) => a.seq!.compareTo(b.seq!));
    return all;
  }

  @override
  Future<void> markOperations(Iterable<String> ids, SyncOpStatus status) =>
      _inOneTransaction(ids, (chunk) {
        return executor.execute(
            'UPDATE $_outbox SET status = ? '
            'WHERE id IN (${_marks(chunk.length)})',
            [status.name, ...chunk]);
      });

  @override
  Future<void> deleteOperations(Iterable<String> ids) =>
      _inOneTransaction(ids, (chunk) {
        return executor.execute(
            'DELETE FROM $_outbox WHERE id IN (${_marks(chunk.length)})',
            chunk);
      }, reserved: 0);

  /// Runs [body] over [ids] in slices; in a transaction only when more than
  /// one statement is needed.
  Future<void> _inOneTransaction(
    Iterable<String> ids,
    Future<void> Function(List<String> chunk) body, {
    int reserved = 1,
  }) {
    final all = ids.toList();
    if (all.isEmpty) return Future.value();
    if (all.length <= _maxVariables - reserved) return body(all);
    return transaction(() => _chunked<String>(all, body, reserved: reserved));
  }

  @override
  Future<int> countOperations({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
  }) async {
    if ((statuses != null && statuses.isEmpty) ||
        (entities != null && entities.isEmpty)) {
      return 0;
    }
    final where = <String>[];
    final args = <Object?>[];
    if (statuses != null) {
      where.add(_among('status', statuses.length));
      args.addAll(statuses.map((s) => s.name));
    }
    if (entities != null) {
      where.add(_among('entity', entities.length));
      args.addAll(entities);
    }
    final rows = await executor.query(
        'SELECT COUNT(*) AS n FROM $_outbox'
        '${where.isEmpty ? '' : ' WHERE ${where.join(' AND ')}'}',
        args);
    return (rows.first['n']! as num).toInt();
  }

  @override
  Future<DateTime?> nextAttemptAfter(DateTime now) async {
    final rows = await executor.query(
        // The `+` keeps SQLite on the small index of the operations that
        // wait for a retry, rather than reading every pending one.
        'SELECT MIN(next_attempt_at) AS t FROM $_outbox '
        'WHERE next_attempt_at > ? AND +status = ?',
        [now.millisecondsSinceEpoch, SyncOpStatus.pending.name]);
    final value = rows.isEmpty ? null : rows.first['t'] as num?;
    return value == null
        ? null
        : DateTime.fromMillisecondsSinceEpoch(value.toInt());
  }

  @override
  Future<void> clearBackoff(DateTime now, {Set<String>? entities}) async {
    if (entities != null && entities.isEmpty) return;
    await executor.execute(
        'UPDATE $_outbox SET next_attempt_at = NULL '
        'WHERE next_attempt_at > ? AND +status = ?'
        '${entities == null ? '' : ' AND +${_among('entity', entities.length)}'}',
        [
          now.millisecondsSinceEpoch,
          SyncOpStatus.pending.name,
          ...?entities,
        ]);
  }

  @override
  Future<Set<String>> localIdsWithOperations(
      String entity, Iterable<String> localIds) async {
    final found = <String>{};
    await _chunked<String>(localIds.toSet(), (chunk) async {
      final rows = await executor.query(
          'SELECT DISTINCT local_id FROM $_outbox '
          'WHERE entity = ? AND local_id IN (${_marks(chunk.length)})',
          [entity, ...chunk]);
      for (final row in rows) {
        found.add(row['local_id']!.toString());
      }
    });
    return found;
  }

  @override
  Future<Map<String, String>> localIdsForServer(
      String entity, Iterable<String> serverIds) async {
    final found = <String, String>{};
    await _chunked<String>(serverIds.toSet(), (chunk) async {
      final rows = await executor.query(
          'SELECT server_id, local_id FROM $_idMap '
          'WHERE entity = ? AND server_id IN (${_marks(chunk.length)})',
          [entity, ...chunk]);
      for (final row in rows) {
        found.putIfAbsent(
            row['server_id']!.toString(), () => row['local_id']!.toString());
      }
    });
    return found;
  }

  @override
  Future<Map<String, IdMapping>> mappingsForLocal(
      String entity, Iterable<String> localIds) async {
    final found = <String, IdMapping>{};
    await _chunked<String>(localIds.toSet(), (chunk) async {
      final rows = await executor.query(
          'SELECT local_id, server_id FROM $_idMap '
          'WHERE entity = ? AND local_id IN (${_marks(chunk.length)})',
          [entity, ...chunk]);
      for (final row in rows) {
        final localId = row['local_id']!.toString();
        found[localId] = IdMapping(
          entity: entity,
          localId: localId,
          serverId: row['server_id']?.toString(),
        );
      }
    });
    return found;
  }

  @override
  Future<void> putMappings(String entity, Map<String, String?> localToServer) {
    if (localToServer.isEmpty) return Future.value();
    return transaction(() => _chunked<MapEntry<String, String?>>(
          localToServer.entries,
          (chunk) => executor.execute(
              'INSERT OR REPLACE INTO $_idMap (entity, local_id, server_id) '
              'VALUES ${List.filled(chunk.length, '(?, ?, ?)').join(', ')}',
              [
                for (final entry in chunk) ...[entity, entry.key, entry.value],
              ]),
          reserved: 0,
          perValue: 3,
        ));
  }
}
