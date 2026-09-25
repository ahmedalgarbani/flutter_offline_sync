import 'dart:async';

import 'operation.dart';

/// Built-in conflict policies.
enum ConflictStrategy {
  /// Keep the unpushed local change; it is pushed (with `force`) and wins.
  /// Safe default for POS-style data entered on the device.
  keepLocal,

  /// Take the server version and drop the unpushed local change.
  serverWins,

  /// Compare the server record's timestamp (`SyncEntityConfig.updatedAtOf`)
  /// with the time of the latest local change. Falls back to [keepLocal]
  /// when the server record has no timestamp.
  lastWriteWins,
}

/// Where the conflict was detected.
enum ConflictSource {
  /// A pulled server record belongs to a record that still has unpushed
  /// operations.
  pull,

  /// The server rejected a push with a conflict (e.g. HTTP 409).
  push,
}

/// Everything a resolver needs to decide.
class SyncConflict {
  const SyncConflict({
    required this.entity,
    required this.localId,
    required this.serverId,
    required this.serverRecord,
    required this.pendingOperations,
    required this.source,
  });

  final String entity;
  final String localId;
  final String? serverId;

  /// Server version, already converted to local form (references hold local
  /// ids). Null when the server did not send one or deleted the record.
  final Map<String, dynamic>? serverRecord;

  /// Unpushed operations for the record, oldest first.
  final List<SyncOperation> pendingOperations;

  final ConflictSource source;

  /// Latest local payload, merged across the pending operations.
  Map<String, dynamic> get localPayload {
    final merged = <String, dynamic>{};
    for (final op in pendingOperations) {
      merged.addAll(op.payload);
    }
    return merged;
  }
}

/// Decision returned by a [ConflictResolver].
sealed class ConflictResolution {
  const ConflictResolution();

  /// Keep the local change and overwrite the server on the next push.
  const factory ConflictResolution.keepLocal() = KeepLocal;

  /// Apply the server version locally and drop the pending operations.
  const factory ConflictResolution.takeServer() = TakeServer;

  /// Store [merged] (local form) locally and push it with `force`.
  const factory ConflictResolution.merge(Map<String, dynamic> merged) =
      MergeResolution;
}

class KeepLocal extends ConflictResolution {
  const KeepLocal();
}

class TakeServer extends ConflictResolution {
  const TakeServer();
}

class MergeResolution extends ConflictResolution {
  const MergeResolution(this.merged);
  final Map<String, dynamic> merged;
}

typedef ConflictResolver = FutureOr<ConflictResolution> Function(
    SyncConflict conflict);

/// Builds a resolver from a [strategy].
ConflictResolver resolverFor(
  ConflictStrategy strategy, {
  DateTime? Function(Map<String, dynamic> record)? updatedAtOf,
}) {
  switch (strategy) {
    case ConflictStrategy.keepLocal:
      return (_) => const ConflictResolution.keepLocal();
    case ConflictStrategy.serverWins:
      return (_) => const ConflictResolution.takeServer();
    case ConflictStrategy.lastWriteWins:
      return (conflict) {
        final record = conflict.serverRecord;
        final serverTime = record == null ? null : updatedAtOf?.call(record);
        if (serverTime == null || conflict.pendingOperations.isEmpty) {
          return const ConflictResolution.keepLocal();
        }
        final localTime = conflict.pendingOperations
            .map((op) => op.updatedAt)
            .reduce((a, b) => a.isAfter(b) ? a : b);
        return serverTime.isAfter(localTime)
            ? const ConflictResolution.takeServer()
            : const ConflictResolution.keepLocal();
      };
  }
}
