import 'dart:math';

/// Exponential backoff with jitter for failed pushes.
///
/// Only failures that reached the server count as attempts. When the device
/// is offline nothing is counted, so going offline never "uses up" retries.
class RetryPolicy {
  const RetryPolicy({
    this.maxAttempts = 8,
    this.baseDelay = const Duration(seconds: 2),
    this.maxDelay = const Duration(minutes: 10),
    this.jitter = 0.2,
  })  : assert(maxAttempts == null || maxAttempts > 0),
        assert(jitter >= 0 && jitter <= 1);

  /// After this many counted attempts the operation is marked failed (it is
  /// never deleted). Null retries forever.
  final int? maxAttempts;

  final Duration baseDelay;
  final Duration maxDelay;

  /// Random +/- fraction applied to each delay so many devices do not retry
  /// in lockstep.
  final double jitter;

  bool isExhausted(int attempts) =>
      maxAttempts != null && attempts >= maxAttempts!;

  /// Delay before the next attempt, given the number of attempts made.
  Duration delayFor(int attempts, [Random? random]) {
    final exponent = min(max(attempts - 1, 0), 20);
    final raw = baseDelay.inMilliseconds * pow(2, exponent);
    final capped = min(raw.toDouble(), maxDelay.inMilliseconds.toDouble());
    final spread = capped * jitter;
    final offset = spread == 0
        ? 0.0
        : ((random ?? Random()).nextDouble() * 2 - 1) * spread;
    return Duration(milliseconds: max(0, (capped + offset).round()));
  }
}
