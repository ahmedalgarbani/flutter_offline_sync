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
///
/// To write your own store, **extend** this class. Only the abstract
/// members are required; the paged and batched members below them have
/// working implementations built on the required ones. Those fallbacks read
/// more than they need, so override them when the outbox or the id map can
/// grow large: the engine never loads the whole outbox itself, it only asks
/// for pages, counts and batches.
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

  // Paged and batched access --------------------------------------------------
  //
  // Everything below has a fallback built on the members above.

  /// At most [limit] operations with a `seq` greater than [afterSeq],
  /// ordered by `seq`: one page of the outbox. Pass the `seq` of the last
  /// operation of a page as [afterSeq] to read the next one.
  ///
  /// [statuses] and [entities] keep only operations with one of those
  /// statuses or of one of those entities (null keeps all). [dueAt] leaves
  /// out operations waiting for a later time (`nextAttemptAt` after it).
  Future<List<SyncOperation>> operationsPage({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
    DateTime? dueAt,
    int afterSeq = 0,
    required int limit,
  }) async {
    if (limit <= 0) return const [];
    final page = <SyncOperation>[];
    for (final op in await operations(statuses: statuses)) {
      if ((op.seq ?? 0) <= afterSeq) continue;
      if (entities != null && !entities.contains(op.entity)) continue;
      final next = op.nextAttemptAt;
      if (dueAt != null && next != null && next.isAfter(dueAt)) continue;
      page.add(op);
      if (page.length == limit) break;
    }
    return page;
  }

  /// Every operation of the records [localIds] of [entity], ordered by
  /// `seq`: [operationsFor] for many records at once.
  Future<List<SyncOperation>> operationsForRecords(
      String entity, Iterable<String> localIds) async {
    final all = <SyncOperation>[];
    for (final localId in localIds.toSet()) {
      all.addAll(await operationsFor(entity, localId));
    }
    return all..sort((a, b) => (a.seq ?? 0).compareTo(b.seq ?? 0));
  }

  /// Sets the status of the operations with [ids], leaving everything else
  /// about them as it is. Unknown ids are ignored.
  Future<void> markOperations(Iterable<String> ids, SyncOpStatus status) =>
      transaction(() async {
        for (final id in ids) {
          final op = await operationById(id);
          if (op != null && op.status != status) {
            await updateOperation(op.copyWith(status: status));
          }
        }
      });

  /// [deleteOperation] for many operations at once.
  Future<void> deleteOperations(Iterable<String> ids) => transaction(() async {
        for (final id in ids) {
          await deleteOperation(id);
        }
      });

  /// Number of operations with one of [statuses] and of one of [entities]
  /// (null counts all).
  Future<int> countOperations({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
  }) async {
    if (entities == null) {
      final counts = await countByStatus();
      var total = 0;
      for (final status in statuses ?? SyncOpStatus.values) {
        total += counts[status] ?? 0;
      }
      return total;
    }
    final ops = await operations(statuses: statuses);
    return ops.where((op) => entities.contains(op.entity)).length;
  }

  /// The earliest `nextAttemptAt` later than [now] among pending
  /// operations: when the next operation waiting out a backoff is due.
  Future<DateTime?> nextAttemptAfter(DateTime now) async {
    DateTime? earliest;
    for (final op in await operations(statuses: {SyncOpStatus.pending})) {
      final next = op.nextAttemptAt;
      if (next != null &&
          next.isAfter(now) &&
          (earliest == null || next.isBefore(earliest))) {
        earliest = next;
      }
    }
    return earliest;
  }

  /// Makes pending operations (of [entities], or all) that wait for a time
  /// after [now] due at once. Their attempt counts are kept.
  Future<void> clearBackoff(DateTime now, {Set<String>? entities}) =>
      transaction(() async {
        for (final op in await operations(statuses: {SyncOpStatus.pending})) {
          final next = op.nextAttemptAt;
          if (next == null || !next.isAfter(now)) continue;
          if (entities != null && !entities.contains(op.entity)) continue;
          await updateOperation(op.copyWith(clearNextAttemptAt: true));
        }
      });

  /// Those of [localIds] that have at least one operation of [entity] in
  /// the outbox, whatever its status.
  Future<Set<String>> localIdsWithOperations(
      String entity, Iterable<String> localIds) async {
    final wanted = localIds.toSet();
    if (wanted.isEmpty) return {};
    final found = <String>{};
    for (final op in await operations()) {
      if (op.entity == entity && wanted.contains(op.localId)) {
        found.add(op.localId);
      }
    }
    return found;
  }

  /// The local id of each of [serverIds] of [entity] that has a mapping,
  /// keyed by server id.
  Future<Map<String, String>> localIdsForServer(
      String entity, Iterable<String> serverIds) async {
    final found = <String, String>{};
    for (final serverId in serverIds) {
      if (found.containsKey(serverId)) continue;
      final mapping = await mappingForServer(entity, serverId);
      if (mapping != null) found[serverId] = mapping.localId;
    }
    return found;
  }

  /// The mapping of each of [localIds] of [entity] that has one, keyed by
  /// local id.
  Future<Map<String, IdMapping>> mappingsForLocal(
      String entity, Iterable<String> localIds) async {
    final found = <String, IdMapping>{};
    for (final localId in localIds) {
      if (found.containsKey(localId)) continue;
      final mapping = await mappingForLocal(entity, localId);
      if (mapping != null) found[localId] = mapping;
    }
    return found;
  }

  /// [putMapping] for many records of [entity] at once ([localToServer]
  /// maps a local id to its server id).
  Future<void> putMappings(String entity, Map<String, String?> localToServer) {
    if (localToServer.isEmpty) return Future.value();
    return transaction(() async {
      for (final entry in localToServer.entries) {
        await putMapping(entity, entry.key, entry.value);
      }
    });
  }
}
