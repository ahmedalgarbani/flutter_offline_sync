import '../core/operation.dart';

/// Link between a local record and its server copy.
class IdMapping {
  const IdMapping({
    required this.entity,
    required this.localId,
    required this.serverId,
  });

  final String entity;
  final String localId;

  /// Null while the record was created locally and not pushed yet.
  final String? serverId;

  bool get isLocalOnly => serverId == null;
}

/// Durable storage for the engine's own metadata: the outbox, the id map
/// and key/value data such as pull cursors.
///
/// Keep it in the same database as your data (see `SqlSyncStore`) so a
/// record write and its outbox entry can commit in one transaction.
abstract class SyncStore {
  /// Creates tables if needed.
  Future<void> init();

  /// Runs [action] atomically. Nested calls join the outer transaction.
  Future<T> transaction<T>(Future<T> Function() action);

  // Outbox ------------------------------------------------------------------

  /// Inserts [operation] and returns it with its `seq` assigned.
  Future<SyncOperation> insertOperation(SyncOperation operation);

  Future<void> updateOperation(SyncOperation operation);

  Future<void> deleteOperation(String id);

  Future<SyncOperation?> operationById(String id);

  /// Operations ordered by `seq`, optionally filtered by status.
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses});

  /// Every operation of one record, ordered by `seq`.
  Future<List<SyncOperation>> operationsFor(String entity, String localId);

  Future<Map<SyncOpStatus, int>> countByStatus();

  /// Moves operations left `inFlight` by a crash back to `pending`.
  Future<void> resetInFlight();

  // Id map ------------------------------------------------------------------

  Future<IdMapping?> mappingForLocal(String entity, String localId);

  Future<IdMapping?> mappingForServer(String entity, String serverId);

  /// Inserts or replaces the mapping of ([entity], [localId]).
  Future<void> putMapping(String entity, String localId, String? serverId);

  Future<void> deleteMapping(String entity, String localId);

  // Key/value ---------------------------------------------------------------

  Future<String?> getMeta(String key);

  /// Stores [value]; null removes the key.
  Future<void> setMeta(String key, String? value);

  /// Removes everything (for example on logout or tenant switch).
  Future<void> clear();
}
