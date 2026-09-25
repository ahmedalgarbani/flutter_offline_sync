import 'operation.dart';

/// What the remote adapter is asked to send.
class PushRequest {
  const PushRequest({
    required this.operation,
    required this.payload,
    this.serverId,
  });

  final SyncOperation operation;

  /// Payload with every declared reference already converted to server ids.
  final Map<String, dynamic> payload;

  /// Server id of the record. Null for creates.
  final String? serverId;

  String get entity => operation.entity;
  String get localId => operation.localId;
  SyncOpType get type => operation.type;

  /// Send this with the request (for example as an `Idempotency-Key`
  /// header) so a retried request after a lost response is not applied
  /// twice by the server.
  String get idempotencyKey => operation.id;

  /// True after a conflict was resolved in favour of the local copy.
  bool get force => operation.force;
}

/// Result of pushing one operation. Adapters either return one of these or
/// throw; thrown errors are classified by `SyncConfig.classifyError`.
sealed class PushOutcome {
  const PushOutcome();

  /// The server accepted the change. For creates, [serverId] is required.
  /// Also use this when the server says the record already exists and you
  /// can tell its id: the local record adopts that server record.
  const factory PushOutcome.success({
    String? serverId,
    Map<String, dynamic>? record,
  }) = PushSuccess;

  /// Temporary server-side failure (5xx, 429, timeout). Counts as an attempt
  /// and is retried with backoff.
  const factory PushOutcome.retry(Object error, {Duration? retryAfter}) =
      PushRetry;

  /// The server could not be reached. The push run stops without counting an
  /// attempt; it resumes on the next trigger.
  const factory PushOutcome.networkError(Object error) = PushNetworkError;

  /// Permanent rejection (validation error). The operation is marked failed
  /// and kept for the user to fix, retry or discard.
  const factory PushOutcome.rejected(Object error) = PushRejected;

  /// The server holds a newer version. [serverRecord] is in server form.
  const factory PushOutcome.conflict({
    Map<String, dynamic>? serverRecord,
    Object? error,
  }) = PushConflict;

  /// Credentials expired. The run stops and the engine reports
  /// `SyncPhase.authRequired` until the next successful sync.
  const factory PushOutcome.unauthorized([Object? error]) = PushUnauthorized;
}

class PushSuccess extends PushOutcome {
  const PushSuccess({this.serverId, this.record});
  final String? serverId;
  final Map<String, dynamic>? record;
}

class PushRetry extends PushOutcome {
  const PushRetry(this.error, {this.retryAfter});
  final Object error;
  final Duration? retryAfter;
}

class PushNetworkError extends PushOutcome {
  const PushNetworkError(this.error);
  final Object error;
}

class PushRejected extends PushOutcome {
  const PushRejected(this.error);
  final Object error;
}

class PushConflict extends PushOutcome {
  const PushConflict({this.serverRecord, this.error});
  final Map<String, dynamic>? serverRecord;
  final Object? error;
}

class PushUnauthorized extends PushOutcome {
  const PushUnauthorized([this.error]);
  final Object? error;
}

/// What the remote adapter is asked to fetch.
class PullRequest {
  const PullRequest({
    required this.entity,
    required this.cursor,
    required this.limit,
  });

  final String entity;

  /// Opaque position returned by the previous page, or null to start over.
  final String? cursor;

  /// Requested page size.
  final int limit;
}

/// One page of server changes.
class PullPage {
  const PullPage({
    required this.records,
    this.deletedServerIds = const [],
    this.nextCursor,
    this.hasMore = false,
  });

  /// Records in server form (references hold server ids).
  final List<Map<String, dynamic>> records;

  /// Server ids deleted since the cursor, when the backend reports them.
  final List<String> deletedServerIds;

  /// Cursor to request the next page, and to store for the next incremental
  /// pull (for example the highest `updatedAt` seen, or a page number).
  final String? nextCursor;

  final bool hasMore;
}

/// Adapters may throw these to control how a failure is handled.
class SyncNetworkException implements Exception {
  const SyncNetworkException([this.message = 'Network unavailable']);
  final Object message;
  @override
  String toString() => 'SyncNetworkException: $message';
}

class SyncUnauthorizedException implements Exception {
  const SyncUnauthorizedException([this.message = 'Unauthorized']);
  final Object message;
  @override
  String toString() => 'SyncUnauthorizedException: $message';
}

class SyncRejectedException implements Exception {
  const SyncRejectedException(this.message);
  final Object message;
  @override
  String toString() => 'SyncRejectedException: $message';
}

/// Default mapping from a thrown error to an outcome.
///
/// Recognizes the exceptions above, plus common transport errors by type
/// name (`SocketException`, `ClientException`, `TimeoutException`, ...), so
/// the package does not need `dart:io` and still works on the web.
PushOutcome defaultClassifyError(Object error) {
  if (error is SyncNetworkException) return PushOutcome.networkError(error);
  if (error is SyncUnauthorizedException) {
    return PushOutcome.unauthorized(error);
  }
  if (error is SyncRejectedException) return PushOutcome.rejected(error);
  const networkTypes = {
    'SocketException',
    'ClientException',
    'HandshakeException',
    'HttpException',
    'TimeoutException',
    'WebSocketException',
  };
  if (networkTypes.contains(error.runtimeType.toString())) {
    return PushOutcome.networkError(error);
  }
  return PushOutcome.retry(error);
}
