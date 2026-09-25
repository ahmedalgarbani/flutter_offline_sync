import 'dart:convert';

/// Kind of change recorded in the outbox.
enum SyncOpType { create, update, delete }

/// Lifecycle of an outbox operation.
enum SyncOpStatus {
  /// Waiting to be pushed (possibly after a backoff delay).
  pending,

  /// Currently being sent. Reset to [pending] on startup after a crash; the
  /// idempotency key protects the server from duplicates.
  inFlight,

  /// Gave up (rejected by the server or too many attempts). Never deleted
  /// automatically: call `retryFailed` or `discard` on the engine.
  failed,
}

/// One durable change waiting to reach the server.
class SyncOperation {
  SyncOperation({
    required this.id,
    this.seq,
    required this.entity,
    required this.localId,
    required this.type,
    required this.payload,
    this.status = SyncOpStatus.pending,
    this.attempts = 0,
    this.force = false,
    required this.createdAt,
    required this.updatedAt,
    this.nextAttemptAt,
    this.lastError,
  });

  /// Unique id, also sent to the server as the idempotency key.
  final String id;

  /// Insertion order assigned by the store. Operations are pushed by [seq].
  final int? seq;

  /// Entity name as registered in `SyncEntityConfig.name`.
  final String entity;

  /// Local primary key of the changed record.
  final String localId;

  final SyncOpType type;

  /// JSON payload in local form (references hold local ids).
  final Map<String, dynamic> payload;

  final SyncOpStatus status;

  /// Number of failed push attempts that counted against the retry policy.
  final int attempts;

  /// Set after a conflict was resolved in favour of the local version: the
  /// remote adapter should overwrite the server copy.
  final bool force;

  final DateTime createdAt;

  /// Last time the payload changed (used by last-write-wins).
  final DateTime updatedAt;

  /// Earliest time of the next attempt (backoff).
  final DateTime? nextAttemptAt;

  /// Last error or the reason the operation is waiting.
  final String? lastError;

  bool get isPending => status == SyncOpStatus.pending;
  bool get isFailed => status == SyncOpStatus.failed;

  SyncOperation copyWith({
    int? seq,
    Map<String, dynamic>? payload,
    SyncOpType? type,
    SyncOpStatus? status,
    int? attempts,
    bool? force,
    DateTime? updatedAt,
    DateTime? nextAttemptAt,
    bool clearNextAttemptAt = false,
    String? lastError,
    bool clearLastError = false,
  }) {
    return SyncOperation(
      id: id,
      seq: seq ?? this.seq,
      entity: entity,
      localId: localId,
      type: type ?? this.type,
      payload: payload ?? this.payload,
      status: status ?? this.status,
      attempts: attempts ?? this.attempts,
      force: force ?? this.force,
      createdAt: createdAt,
      updatedAt: updatedAt ?? this.updatedAt,
      nextAttemptAt:
          clearNextAttemptAt ? null : (nextAttemptAt ?? this.nextAttemptAt),
      lastError: clearLastError ? null : (lastError ?? this.lastError),
    );
  }

  /// Row form used by SQL stores.
  Map<String, Object?> toRow() => {
        'id': id,
        'entity': entity,
        'local_id': localId,
        'op_type': type.name,
        'payload': jsonEncode(payload),
        'status': status.name,
        'attempts': attempts,
        'force': force ? 1 : 0,
        'created_at': createdAt.millisecondsSinceEpoch,
        'updated_at': updatedAt.millisecondsSinceEpoch,
        'next_attempt_at': nextAttemptAt?.millisecondsSinceEpoch,
        'last_error': lastError,
      };

  factory SyncOperation.fromRow(Map<String, Object?> row) {
    DateTime? time(Object? v) => v == null
        ? null
        : DateTime.fromMillisecondsSinceEpoch((v as num).toInt());
    return SyncOperation(
      id: row['id']! as String,
      seq: (row['seq'] as num?)?.toInt(),
      entity: row['entity']! as String,
      localId: row['local_id']! as String,
      type: SyncOpType.values.byName(row['op_type']! as String),
      payload: Map<String, dynamic>.from(
          jsonDecode(row['payload']! as String) as Map),
      status: SyncOpStatus.values.byName(row['status']! as String),
      attempts: (row['attempts'] as num?)?.toInt() ?? 0,
      force: (row['force'] as num?)?.toInt() == 1,
      createdAt: time(row['created_at'])!,
      updatedAt: time(row['updated_at'])!,
      nextAttemptAt: time(row['next_attempt_at']),
      lastError: row['last_error'] as String?,
    );
  }

  @override
  String toString() => 'SyncOperation(#$seq ${type.name} $entity/$localId '
      '${status.name}, attempts: $attempts)';
}
