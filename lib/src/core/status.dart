/// What the engine is doing right now.
enum SyncPhase {
  /// Nothing running. Check [SyncStatus.pendingCount] for unsynced work.
  idle,
  pushing,
  pulling,

  /// The last run could not reach the server.
  offline,

  /// The server rejected the credentials; sync resumes after re-login.
  authRequired,

  /// `SyncConfig.canSync` returned false (e.g. no session yet).
  paused,
}

/// Snapshot of the engine state for the UI.
///
/// Two snapshots with the same values are equal, so listeners are only
/// notified when something they can show actually changed.
class SyncStatus {
  const SyncStatus({
    this.phase = SyncPhase.idle,
    this.isOnline = true,
    this.pendingCount = 0,
    this.failedCount = 0,
    this.currentEntity,
    this.lastSyncedAt,
    this.lastError,
    this.nextRetryAt,
  });

  final SyncPhase phase;
  final bool isOnline;

  /// Operations still waiting to be pushed.
  final int pendingCount;

  /// Operations that gave up and need attention.
  final int failedCount;

  /// Entity being pushed or pulled.
  final String? currentEntity;

  /// End of the last run that was not cut short (by being offline,
  /// unauthorized, paused or an unexpected error).
  final DateTime? lastSyncedAt;

  final String? lastError;

  /// When the first operation waiting out a backoff delay (after a
  /// temporary failure) becomes due. A started engine retries it at that
  /// time by itself. Null when no operation is backing off.
  final DateTime? nextRetryAt;

  bool get isSyncing =>
      phase == SyncPhase.pushing || phase == SyncPhase.pulling;

  /// True when every local change has reached the server.
  bool get isUpToDate => pendingCount == 0 && failedCount == 0;

  SyncStatus copyWith({
    SyncPhase? phase,
    bool? isOnline,
    int? pendingCount,
    int? failedCount,
    String? currentEntity,
    bool clearCurrentEntity = false,
    DateTime? lastSyncedAt,
    String? lastError,
    bool clearLastError = false,
    DateTime? nextRetryAt,
    bool clearNextRetryAt = false,
  }) {
    return SyncStatus(
      phase: phase ?? this.phase,
      isOnline: isOnline ?? this.isOnline,
      pendingCount: pendingCount ?? this.pendingCount,
      failedCount: failedCount ?? this.failedCount,
      currentEntity:
          clearCurrentEntity ? null : (currentEntity ?? this.currentEntity),
      lastSyncedAt: lastSyncedAt ?? this.lastSyncedAt,
      lastError: clearLastError ? null : (lastError ?? this.lastError),
      nextRetryAt: clearNextRetryAt ? null : (nextRetryAt ?? this.nextRetryAt),
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SyncStatus &&
          other.phase == phase &&
          other.isOnline == isOnline &&
          other.pendingCount == pendingCount &&
          other.failedCount == failedCount &&
          other.currentEntity == currentEntity &&
          other.lastSyncedAt == lastSyncedAt &&
          other.lastError == lastError &&
          other.nextRetryAt == nextRetryAt;

  @override
  int get hashCode => Object.hash(phase, isOnline, pendingCount, failedCount,
      currentEntity, lastSyncedAt, lastError, nextRetryAt);

  @override
  String toString() => 'SyncStatus(${phase.name}, online: $isOnline, '
      'pending: $pendingCount, failed: $failedCount)';
}

/// Sync state of a single record, for per-row badges.
enum RecordSyncState {
  /// Every change of the record reached the server.
  synced,

  /// Changes are waiting in the outbox.
  pending,

  /// A change failed and needs attention.
  failed,
}

/// Why a run stopped early.
enum SyncAbortReason { offline, unauthorized, paused }

/// Summary of one run.
class SyncRunResult {
  SyncRunResult();

  int pushed = 0;
  int failed = 0;

  /// Operations left pending by the run: waiting for a parent, for a
  /// backoff delay, for an earlier operation of the same record, or for the
  /// connection to come back.
  int waiting = 0;
  int pulled = 0;
  final List<String> errors = [];
  SyncAbortReason? abortedBy;
  Duration duration = Duration.zero;

  bool get completed => abortedBy == null;
  bool get isSuccess => completed && failed == 0 && errors.isEmpty;

  @override
  String toString() => 'SyncRunResult(pushed: $pushed, failed: $failed, '
      'waiting: $waiting, pulled: $pulled, errors: ${errors.length}, '
      'aborted: ${abortedBy?.name}, ${duration.inMilliseconds}ms)';
}

/// Fine-grained notifications, e.g. for logging or refreshing a list.
class SyncEvent {
  const SyncEvent(this.type, {this.entity, this.localId, this.message});

  final SyncEventType type;
  final String? entity;
  final String? localId;
  final String? message;

  @override
  String toString() =>
      'SyncEvent(${type.name} $entity/$localId${message == null ? '' : ': $message'})';
}

enum SyncEventType {
  /// A run started.
  runStarted,

  /// A run ended; [SyncEvent.message] summarizes its [SyncRunResult].
  runFinished,

  /// A local change was recorded (or merged into a queued one).
  operationRecorded,

  /// The server accepted a change; [SyncEvent.message] is the server id.
  operationPushed,

  /// A change was marked failed; [SyncEvent.message] is the error.
  operationFailed,

  /// A failed change was queued again by `retryFailed`.
  operationRetried,

  /// A change was removed by `discard`.
  operationDiscarded,

  /// A server record was written locally.
  recordPulled,

  /// The server deleted a record and the local copy was removed.
  recordDeletedByServer,

  /// A conflict resolver decided; [SyncEvent.message] names the decision.
  conflictResolved,

  /// `SyncEngine.clear` wiped the outbox, id map and cursors.
  storeCleared,
}
