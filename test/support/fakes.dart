import 'dart:async';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

/// No automatic triggers: tests start every run themselves.
const manualConfig = SyncConfig(
  syncOnStart: false,
  syncOnResume: false,
  syncOnReconnect: false,
  autoPushAfterWrite: false,
  periodicInterval: null,
);

/// A backend kept in memory, one table per entity. Server ids are integers
/// starting at 100, so they never look like the test's local ids.
class FakeServer {
  final Map<String, Map<String, Map<String, dynamic>>> tables = {};
  final List<PushRequest> pushes = [];
  final List<PullRequest> pulls = [];
  int _nextId = 100;

  /// Replaces the default handling of a push when it returns an outcome.
  FutureOr<PushOutcome?> Function(PushRequest request)? onPush;

  Map<String, Map<String, dynamic>> table(String entity) =>
      tables.putIfAbsent(entity, () => {});

  /// Adds a record as if another device had created it.
  String seed(String entity, Map<String, dynamic> record) {
    final id = '${_nextId++}';
    table(entity)[id] = {...record, 'id': int.parse(id)};
    return id;
  }

  RemoteAdapter adapter(String entity) => CallbackRemoteAdapter(
        push: (request) => _push(entity, request),
        pull: (request) => _pull(entity, request),
      );

  Future<PushOutcome> _push(String entity, PushRequest request) async {
    pushes.add(request);
    final override = await onPush?.call(request);
    if (override != null) return override;
    final rows = table(entity);
    switch (request.type) {
      case SyncOpType.create:
        final id = '${_nextId++}';
        rows[id] = {...request.payload, 'id': int.parse(id)};
        return PushOutcome.success(serverId: id);
      case SyncOpType.update:
        final id = request.serverId!;
        if (!rows.containsKey(id)) {
          return PushOutcome.rejected('$entity/$id does not exist');
        }
        rows[id] = {...rows[id]!, ...request.payload};
        return const PushOutcome.success();
      case SyncOpType.delete:
        rows.remove(request.serverId);
        return const PushOutcome.success();
    }
  }

  /// Pages through the table in insertion order; the cursor is an offset.
  Future<PullPage> _pull(String entity, PullRequest request) async {
    pulls.add(request);
    final rows = table(entity).values.toList();
    final offset = int.tryParse(request.cursor ?? '') ?? 0;
    final page = rows.skip(offset).take(request.limit).toList();
    final next = offset + page.length;
    return PullPage(
      records: [for (final row in page) Map.of(row)],
      hasMore: next < rows.length,
      nextCursor: '$next',
    );
  }
}

/// The app's own tables, with a [LocalAdapter] per entity.
class FakeLocalDb {
  final Map<String, Map<String, Map<String, dynamic>>> tables = {};
  final List<String> serverIdsAssigned = [];
  final List<String> pushed = [];
  int _next = 1;

  Map<String, Map<String, dynamic>> table(String entity) =>
      tables.putIfAbsent(entity, () => {});

  LocalAdapter adapter(String entity) => CallbackLocalAdapter(
        applyRemote: (record, {localId, required serverId}) async {
          final id = localId ?? 'pulled-${_next++}';
          table(entity)[id] = Map.of(record);
          return id;
        },
        applyRemoteDelete: (localId, {required serverId}) async {
          table(entity).remove(localId);
        },
        onServerIdAssigned: (localId, serverId, {serverRecord}) async {
          serverIdsAssigned.add('$entity/$localId=$serverId');
        },
        onPushed: (operation, {serverId}) async {
          pushed.add('${operation.type.name} $entity/${operation.localId}');
        },
      );
}

/// Builds an entity backed by [server] and [db].
SyncEntityConfig entity(
  String name,
  FakeServer server,
  FakeLocalDb db, {
  List<SyncReference> references = const [],
  PullMode pullMode = PullMode.none,
  ConflictStrategy conflictStrategy = ConflictStrategy.keepLocal,
  ConflictResolver? conflictResolver,
  bool coalesce = true,
  UnknownReferencePolicy unknownReferencePolicy =
      UnknownReferencePolicy.passThrough,
  Object Function(String serverId)? serverIdToJson,
  Object? Function(Map<String, dynamic> record)? clientIdOf,
  DateTime? Function(Map<String, dynamic> record)? updatedAtOf,
  bool Function(Map<String, dynamic> record)? isDeletedOf,
  RemoteAdapter? remote,
  LocalAdapter? local,
  int pullPageSize = 500,
  int pushBatchSize = 1,
}) =>
    SyncEntityConfig(
      name: name,
      remote: remote ?? server.adapter(name),
      local: local ?? db.adapter(name),
      references: references,
      pullMode: pullMode,
      conflictStrategy: conflictStrategy,
      conflictResolver: conflictResolver,
      coalesce: coalesce,
      unknownReferencePolicy: unknownReferencePolicy,
      serverIdToJson: serverIdToJson,
      clientIdOf: clientIdOf,
      updatedAtOf: updatedAtOf,
      isDeletedOf: isDeletedOf,
      pullPageSize: pullPageSize,
      pushBatchSize: pushBatchSize,
    );

/// A [SqlExecutor] for sqflite that routes statements made inside
/// [transaction] to the open transaction through a zone.
class SqfliteExecutor implements SqlExecutor {
  SqfliteExecutor(this.db);

  final Database db;

  /// Per instance, so a transaction on another database is never joined.
  final Object _txn = Object();

  DatabaseExecutor get _current =>
      Zone.current[_txn] as DatabaseExecutor? ?? db;

  @override
  Future<void> execute(String sql, [List<Object?> args = const []]) =>
      _current.execute(sql, args);

  @override
  Future<List<Map<String, Object?>>> query(String sql,
          [List<Object?> args = const []]) =>
      _current.rawQuery(sql, args);

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    if (Zone.current[_txn] != null) return action();
    return db.transaction((txn) => runZoned(action, zoneValues: {_txn: txn}));
  }
}

/// A fresh in-memory SQLite database.
Future<Database> openTestDatabase() {
  sqfliteFfiInit();
  return databaseFactoryFfiNoIsolate.openDatabase(inMemoryDatabasePath,
      options: OpenDatabaseOptions(singleInstance: false));
}

/// An [InMemorySyncStore] that counts the calls made to it.
class CountingStore extends InMemorySyncStore {
  final Map<String, int> calls = {};

  int count(String name) => calls[name] ?? 0;

  void _called(String name) => calls[name] = count(name) + 1;

  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) {
    _called('operations');
    return super.operations(statuses: statuses);
  }

  @override
  Future<List<SyncOperation>> operationsFor(String entity, String localId) {
    _called('operationsFor');
    return super.operationsFor(entity, localId);
  }

  @override
  Future<Map<SyncOpStatus, int>> countByStatus() {
    _called('countByStatus');
    return super.countByStatus();
  }

  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) {
    _called('mappingForLocal');
    return super.mappingForLocal(entity, localId);
  }

  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) {
    _called('mappingForServer');
    return super.mappingForServer(entity, serverId);
  }

  @override
  Future<void> putMapping(String entity, String localId, String? serverId) {
    _called('putMapping');
    return super.putMapping(entity, localId, serverId);
  }

  @override
  Future<void> putMappings(String entity, Map<String, String?> localToServer) {
    _called('putMappings');
    return super.putMappings(entity, localToServer);
  }

  @override
  Future<Map<String, String>> localIdsForServer(
      String entity, Iterable<String> serverIds) {
    _called('localIdsForServer');
    return super.localIdsForServer(entity, serverIds);
  }

  @override
  Future<Set<String>> localIdsWithOperations(
      String entity, Iterable<String> localIds) {
    _called('localIdsWithOperations');
    return super.localIdsWithOperations(entity, localIds);
  }

  @override
  Future<List<SyncOperation>> operationsPage({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
    DateTime? dueAt,
    int afterSeq = 0,
    required int limit,
  }) {
    _called('operationsPage');
    return super.operationsPage(
        statuses: statuses,
        entities: entities,
        dueAt: dueAt,
        afterSeq: afterSeq,
        limit: limit);
  }

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    _called('transaction');
    return super.transaction(action);
  }
}

/// Lets queued microtasks and zero-duration timers run.
Future<void> settle([Duration duration = Duration.zero]) =>
    Future<void>.delayed(duration);
