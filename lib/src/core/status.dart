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
class SyncStatus {
  const SyncStatus({
    this.phase = SyncPhase.idle,
    this.isOnline = true,
    this.pendingCount = 0,
    this.failedCount = 0,
    this.currentEntity,
    this.lastSyncedAt,
    this.lastError,
  });

  final SyncPhase phase;
  final bool isOnline;

  /// Operations still waiting to be pushed.
  final int pendingCount;

  /// Operations that gave up and need attention.
  final int failedCount;

  /// Entity being pushed or pulled.
  final String? currentEntity;

  /// End of the last run that pushed and pulled without being cut short.
  final DateTime? lastSyncedAt;

  final String? lastError;

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
    );
  }

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

  /// Operations skipped this run: waiting for a parent, for backoff, or for
  /// an earlier operation of the same record.
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
  runStarted,
  runFinished,
  operationRecorded,
  operationPushed,
  operationFailed,
  recordPulled,
  recordDeletedByServer,
  conflictResolved,
}
