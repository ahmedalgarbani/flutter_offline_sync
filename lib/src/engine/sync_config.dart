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
    this.periodicInterval = const Duration(minutes: 5),
    this.syncOnStart = true,
    this.syncOnResume = true,
    this.syncOnReconnect = true,
    this.autoPushAfterWrite = true,
    this.pullOnSync = true,
    this.maxPushPasses = 10,
    this.canSync,
    this.classifyError = defaultClassifyError,
    this.onOperationFailed,
    this.logger,
    this.clock,
  }) : assert(maxPushPasses > 0);

  final RetryPolicy retryPolicy;

  /// Delay that groups bursts of writes into one push.
  final Duration debounce;

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

  /// Gate checked before each run, e.g. "user is logged in and the session
  /// is valid". Returning false pauses sync without touching the outbox.
  final FutureOr<bool> Function()? canSync;

  /// Maps an error thrown by an adapter to an outcome.
  final PushOutcome Function(Object error) classifyError;

  /// Called when an operation is marked failed (rejected, or out of
  /// attempts). Show it to the user; it stays in the outbox.
  final void Function(SyncOperation operation, Object error)?
      onOperationFailed;

  final SyncLogger? logger;

  /// Time source, replaceable in tests.
  final DateTime Function()? clock;
}
