import 'dart:async';

import '../core/operation.dart';
import '../core/outcomes.dart';
import '../core/retry.dart';

enum SyncLogLevel { debug, info, warning, error }

typedef SyncLogger = void Function(
  SyncLogLevel level,
  String message, [
  Object? error,
  StackTrace? stackTrace,
]);

/// Engine-wide behaviour. Every field has a sensible default.
class SyncConfig {
  const SyncConfig({
    this.retryPolicy = const RetryPolicy(),
    this.debounce = const Duration(milliseconds: 800),
    this.maxDebounce = const Duration(seconds: 5),
    this.periodicInterval = const Duration(minutes: 5),
    this.syncOnStart = true,
    this.syncOnResume = true,
    this.syncOnReconnect = true,
    this.autoPushAfterWrite = true,
    this.pullOnSync = true,
    this.maxPushPasses = 10,
    this.outboxPageSize = 200,
    this.serverUnavailableThreshold = 3,
    this.canSync,
    this.onUnauthorized,
    this.classifyError = defaultClassifyError,
    this.onOperationFailed,
    this.logger,
    this.clock,
  })  : assert(maxPushPasses > 0),
        assert(outboxPageSize > 0),
        assert(serverUnavailableThreshold == null ||
            serverUnavailableThreshold > 0);

  final RetryPolicy retryPolicy;

  /// Delay that groups bursts of writes into one push.
  final Duration debounce;

  /// Longest an automatic sync can be put off by writes that keep arriving
  /// less than [debounce] apart (a form saving on every keystroke, a
  /// tracker logging every second). Once this much time has passed since
  /// the first postponed request, the sync starts anyway. Null lets writes
  /// postpone it indefinitely.
  final Duration? maxDebounce;

  /// Background full sync interval. Null disables it.
  final Duration? periodicInterval;

  final bool syncOnStart;

  /// Sync when the app returns to the foreground.
  final bool syncOnResume;

  /// Sync when the connectivity source reports online.
  final bool syncOnReconnect;

  /// Push shortly after every recorded write.
  final bool autoPushAfterWrite;

  /// Whether automatic and `syncNow()` runs also pull.
  final bool pullOnSync;

  /// Push passes per run. A later pass picks up operations that became
  /// ready because their parent was pushed in an earlier pass.
  final int maxPushPasses;

  /// How many operations are read from the outbox at a time while pushing.
  /// The outbox is never loaded whole, so memory use does not grow with it.
  /// (A page always holds at least one full `pushBatchSize` batch.)
  final int outboxPageSize;

  /// After this many pushes in a row answered "the server is unavailable"
  /// (HTTP 429, 502, 503 or 504 with `RestRemoteAdapter`, or
  /// `PushOutcome.retry(..., serverUnavailable: true)`), the run stops and
  /// reports `SyncPhase.serverUnavailable`, as it would stop for a lost
  /// connection. The rest of the outbox is not sent, so an outage does not
  /// use up the attempts of every operation, and a server that is
  /// struggling is not hit with the whole backlog of every device.
  ///
  /// Sync then resumes by itself after the retry policy's delay (or the
  /// server's `Retry-After`, when longer); the delay grows while the server
  /// stays unavailable, and one such answer is then enough to pause again.
  /// `syncNow(ignoreBackoff: true)` tries at once. Null never stops.
  final int? serverUnavailableThreshold;

  /// Gate checked before each run, e.g. "user is logged in and the session
  /// is valid". Returning false pauses sync without touching the outbox.
  final FutureOr<bool> Function()? canSync;

  /// Called when the server reports expired credentials during a run (a
  /// push or pull answered `unauthorized`), at most once per run. Refresh
  /// the session and return true to retry at once; return false (or throw)
  /// to stop with `SyncPhase.authRequired` until the next sync request.
  ///
  /// ```dart
  /// onUnauthorized: () => auth.refreshToken(), // Future<bool>
  /// ```
  final FutureOr<bool> Function()? onUnauthorized;

  /// Maps an error thrown by an adapter to an outcome.
  final PushOutcome Function(Object error) classifyError;

  /// Called when an operation is marked failed (rejected, or out of
  /// attempts). Show it to the user; it stays in the outbox.
  final void Function(SyncOperation operation, Object error)? onOperationFailed;

  final SyncLogger? logger;

  /// Time source, replaceable in tests.
  final DateTime Function()? clock;
}
