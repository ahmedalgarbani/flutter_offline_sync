import '../core/ids.dart';
import '../core/operation.dart';
import '../core/outcomes.dart';
import 'remote_adapter.dart';

/// HTTP request built by [RestRemoteAdapter].
class RestRequest {
  const RestRequest({
    required this.method,
    required this.path,
    this.query = const {},
    this.body,
    this.headers = const {},
  });

  final String method;
  final String path;
  final Map<String, String> query;
  final Object? body;
  final Map<String, String> headers;
}

/// HTTP response handed back to [RestRemoteAdapter]. [body] is the decoded
/// JSON.
class RestResponse {
  const RestResponse({
    required this.statusCode,
    this.body,
    this.headers = const {},
  });

  final int statusCode;
  final Object? body;
  final Map<String, String> headers;
}

/// Sends a request with your HTTP client (http, dio, ...). Throw on
/// transport failures; `SocketException`, `TimeoutException` and
/// `ClientException` are recognized as "offline" automatically.
typedef RestTransport = Future<RestResponse> Function(RestRequest request);

/// How pages are requested and how the next cursor is derived.
abstract class PullPagination {
  const PullPagination();

  Map<String, String> query(String? cursor, int limit);

  PullPage toPage(
      List<Map<String, dynamic>> records, String? cursor, int limit);
}

/// `?page=1&count=500`. Use it with `PullMode.fullRefresh`: page numbers
/// cannot resume an incremental pull.
class PagePagination extends PullPagination {
  const PagePagination({
    this.pageParam = 'page',
    this.sizeParam = 'count',
    this.firstPage = 1,
  });

  final String pageParam;
  final String sizeParam;
  final int firstPage;

  @override
  Map<String, String> query(String? cursor, int limit) => {
        pageParam: cursor ?? '$firstPage',
        sizeParam: '$limit',
      };

  @override
  PullPage toPage(
      List<Map<String, dynamic>> records, String? cursor, int limit) {
    final page = int.tryParse(cursor ?? '') ?? firstPage;
    final hasMore = records.length >= limit;
    return PullPage(
      records: records,
      hasMore: hasMore,
      nextCursor: hasMore ? '${page + 1}' : null,
    );
  }
}

/// `?updatedSince=<timestamp>&count=500`, for backends that can filter by
/// modification time. The cursor is the newest timestamp seen, so pulls
/// resume where they stopped. Use it with `PullMode.incremental`.
class UpdatedSincePagination extends PullPagination {
  const UpdatedSincePagination({
    required this.updatedAtOf,
    this.sinceParam = 'updatedSince',
    this.sizeParam = 'count',
  });

  final String? Function(Map<String, dynamic> record) updatedAtOf;
  final String sinceParam;
  final String sizeParam;

  @override
  Map<String, String> query(String? cursor, int limit) => {
        if (cursor != null) sinceParam: cursor,
        sizeParam: '$limit',
      };

  @override
  PullPage toPage(
      List<Map<String, dynamic>> records, String? cursor, int limit) {
    String? newest = cursor;
    DateTime? newestTime = cursor == null ? null : DateTime.tryParse(cursor);
    for (final record in records) {
      final raw = updatedAtOf(record);
      final time = raw == null ? null : DateTime.tryParse(raw);
      if (time != null && (newestTime == null || time.isAfter(newestTime))) {
        newestTime = time;
        newest = raw;
      }
    }
    return PullPage(
      records: records,
      hasMore: records.length >= limit,
      nextCursor: newest,
    );
  }
}

/// A [RemoteAdapter] for conventional REST resources:
///
/// | change | request                     |
/// |--------|-----------------------------|
/// | create | `POST   {resource}`         |
/// | update | `PUT    {resource}/{id}`    |
/// | delete | `DELETE {resource}/{id}`    |
/// | pull   | `GET    {resource}?page=..` |
///
/// Every part can be overridden, including how an envelope such as
/// `{"success": false, "message": "..."}` returned with HTTP 200 is
/// recognized as an error.
class RestRemoteAdapter extends RemoteAdapter {
  RestRemoteAdapter({
    required this.transport,
    required this.resource,
    String Function(PushRequest request)? createPath,
    String Function(PushRequest request)? updatePath,
    String Function(PushRequest request)? deletePath,
    String? pullPath,
    this.updateMethod = 'PUT',
    this.sendDeleteBody = false,
    this.idempotencyHeader = 'Idempotency-Key',
    this.forceHeader,
    this.pagination = const PagePagination(),
    this.pullQuery = const {},
    this.encodePayload,
    Object? Function(Object? body)? extractServerId,
    Map<String, dynamic>? Function(Object? body)? extractRecord,
    List<Map<String, dynamic>> Function(Object? body)? extractRecords,
    String? Function(RestResponse response)? applicationError,
    bool Function(RestResponse response)? isAlreadyExists,
  })  : createPath = createPath ?? ((_) => resource),
        updatePath = updatePath ?? ((r) => '$resource/${r.serverId}'),
        deletePath = deletePath ?? ((r) => '$resource/${r.serverId}'),
        pullPath = pullPath ?? resource,
        extractServerId = extractServerId ?? defaultExtractServerId,
        extractRecord = extractRecord ?? defaultExtractRecord,
        extractRecords = extractRecords ?? defaultExtractRecords,
        applicationError = applicationError ?? defaultApplicationError,
        isAlreadyExists = isAlreadyExists ?? ((_) => false);

  final RestTransport transport;
  final String resource;
  final String Function(PushRequest request) createPath;
  final String Function(PushRequest request) updatePath;
  final String Function(PushRequest request) deletePath;
  final String pullPath;
  final String updateMethod;
  final bool sendDeleteBody;

  /// Header carrying the operation id. Null to omit.
  final String? idempotencyHeader;

  /// Header set to `true` when a conflict was resolved in favour of the
  /// local copy. Null to omit.
  final String? forceHeader;

  final PullPagination pagination;
  final Map<String, String> pullQuery;

  /// Converts the payload to the request body. Defaults to the payload.
  final Object? Function(PushRequest request)? encodePayload;

  final Object? Function(Object? body) extractServerId;
  final Map<String, dynamic>? Function(Object? body) extractRecord;
  final List<Map<String, dynamic>> Function(Object? body) extractRecords;

  /// Returns an error message when a 2xx response is actually a failure.
  final String? Function(RestResponse response) applicationError;

  /// True when the server refused a create because the record already
  /// exists (e.g. the first request succeeded but its response was lost).
  /// The record is then adopted if the response carries its id.
  final bool Function(RestResponse response) isAlreadyExists;

  @override
  Future<PushOutcome> push(PushRequest request) async {
    final headers = <String, String>{
      if (idempotencyHeader != null) idempotencyHeader!: request.idempotencyKey,
      if (forceHeader != null && request.force) forceHeader!: 'true',
    };
    final body = encodePayload?.call(request) ?? request.payload;
    final RestRequest http = switch (request.type) {
      SyncOpType.create => RestRequest(
          method: 'POST',
          path: createPath(request),
          body: body,
          headers: headers),
      SyncOpType.update => RestRequest(
          method: updateMethod,
          path: updatePath(request),
          body: body,
          headers: headers),
      SyncOpType.delete => RestRequest(
          method: 'DELETE',
          path: deletePath(request),
          body: sendDeleteBody ? body : null,
          headers: headers),
    };
    final response = await transport(http);
    return classify(request, response);
  }

  /// Maps a response to an outcome. Override for unusual APIs.
  PushOutcome classify(PushRequest request, RestResponse response) {
    final code = response.statusCode;
    if (code == 401 || code == 403) {
      return PushOutcome.unauthorized(_message(response));
    }
    if (code == 409) {
      return PushOutcome.conflict(
          serverRecord: extractRecord(response.body),
          error: _message(response));
    }
    if (code == 404 && request.type == SyncOpType.delete) {
      // Already gone: the goal of the delete is reached.
      return const PushOutcome.success();
    }
    if (code == 408 || code == 425 || code == 429 || code >= 500) {
      final retryAfter = int.tryParse(response.headers['retry-after'] ??
          response.headers['Retry-After'] ??
          '');
      return PushOutcome.retry(_message(response),
          retryAfter:
              retryAfter == null ? null : Duration(seconds: retryAfter));
    }

    final failure =
        code >= 400 ? _message(response) : applicationError(response);
    if (failure != null) {
      if (request.type == SyncOpType.create && isAlreadyExists(response)) {
        final id = SyncIds.normalize(extractServerId(response.body));
        if (id != null) {
          return PushOutcome.success(
              serverId: id, record: extractRecord(response.body));
        }
      }
      return PushOutcome.rejected(failure);
    }

    return PushOutcome.success(
      serverId: SyncIds.normalize(extractServerId(response.body)),
      record: extractRecord(response.body),
    );
  }

  @override
  Future<PullPage> pull(PullRequest request) async {
    final response = await transport(RestRequest(
      method: 'GET',
      path: pullPath,
      query: {
        ...pullQuery,
        ...pagination.query(request.cursor, request.limit),
      },
    ));
    final code = response.statusCode;
    if (code == 401 || code == 403) {
      throw SyncUnauthorizedException(_message(response));
    }
    final failure =
        code >= 400 ? _message(response) : applicationError(response);
    if (failure != null) throw SyncRejectedException(failure);
    return pagination.toPage(
        extractRecords(response.body), request.cursor, request.limit);
  }

  String _message(RestResponse response) {
    final body = response.body;
    if (body is Map) {
      final message = body['message'] ?? body['Message'] ?? body['error'];
      final errors = body['errors'] ?? body['Errors'];
      if (message != null || errors != null) {
        return [
          if (message != null) '$message',
          if (errors != null) '$errors',
        ].join(' ');
      }
    }
    return 'HTTP ${response.statusCode}';
  }

  static Object? defaultExtractServerId(Object? body) {
    if (body is! Map) return null;
    final data = body['data'] ?? body['Data'];
    if (data is Map) {
      final nested = data['id'] ?? data['Id'];
      if (nested != null) return nested;
    }
    return body['id'] ?? body['Id'];
  }

  static Map<String, dynamic>? defaultExtractRecord(Object? body) {
    if (body is! Map) return null;
    final data = body['data'] ?? body['Data'];
    if (data is Map) return Map<String, dynamic>.from(data);
    return null;
  }

  static List<Map<String, dynamic>> defaultExtractRecords(Object? body) {
    Object? list = body;
    if (body is Map) {
      list = body['data'] ??
          body['Data'] ??
          body['items'] ??
          body['results'] ??
          body['dataList'];
    }
    if (list is! List) return const [];
    return [
      for (final item in list)
        if (item is Map) Map<String, dynamic>.from(item),
    ];
  }

  /// Treats `{"success": false}` or `{"status": false}` as an error.
  static String? defaultApplicationError(RestResponse response) {
    final body = response.body;
    if (body is! Map) return null;
    final success = body['success'] ?? body['Success'];
    final status = body['status'] ?? body['Status'];
    if (success == false || status == false) {
      final message = body['message'] ?? body['Message'];
      final errors = body['errors'] ?? body['Errors'];
      return [
        message ?? 'Request rejected',
        if (errors != null) '$errors',
      ].join(' ');
    }
    return null;
  }
}
