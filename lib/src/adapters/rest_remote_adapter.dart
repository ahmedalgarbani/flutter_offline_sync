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

  @override
  String toString() => 'RestRequest($method $path'
      '${query.isEmpty ? '' : ' $query'})';
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

  bool get isSuccessful => statusCode >= 200 && statusCode < 300;

  /// The value of the header [name], whatever the case of its key.
  String? header(String name) {
    final exact = headers[name];
    if (exact != null) return exact;
    final lower = name.toLowerCase();
    for (final entry in headers.entries) {
      if (entry.key.toLowerCase() == lower) return entry.value;
    }
    return null;
  }

  @override
  String toString() => 'RestResponse($statusCode)';
}

/// Sends a request with your HTTP client (http, dio, ...). Throw on
/// transport failures; `SocketException`, `TimeoutException` and
/// `ClientException` are recognized as "offline" automatically (see
/// [isNetworkError]).
typedef RestTransport = Future<RestResponse> Function(RestRequest request);

/// How pages are requested and how the next cursor is derived.
abstract class PullPagination {
  const PullPagination();

  /// Query parameters for the page that starts at [cursor] (null for the
  /// first page).
  Map<String, String> query(String? cursor, int limit);

  /// Builds the page from the records of a response.
  PullPage toPage(
      List<Map<String, dynamic>> records, String? cursor, int limit);

  /// Builds the page from the whole response. Override it to read paging
  /// metadata (a next cursor, a total, a "has more" flag) from the body or
  /// the headers. The default calls [toPage].
  PullPage toPageFromResponse(RestResponse response,
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      toPage(records, cursor, limit);
}

/// Reads from a response whether another page exists. Return null when the
/// response does not say, to fall back to "the page was full".
typedef HasMoreOf = bool? Function(RestResponse response);

/// `?page=1&count=500`. Use it with `PullMode.fullRefresh`: page numbers
/// cannot resume an incremental pull.
///
/// A page is assumed to be the last one when it holds fewer than the
/// requested number of records. If the server caps the page size below
/// `SyncEntityConfig.pullPageSize`, lower `pullPageSize` to that cap or
/// pass [hasMoreOf]; otherwise only the first page would be pulled.
class PagePagination extends PullPagination {
  const PagePagination({
    this.pageParam = 'page',
    this.sizeParam = 'count',
    this.firstPage = 1,
    this.hasMoreOf,
  });

  final String pageParam;
  final String sizeParam;
  final int firstPage;

  /// For example `(r) => (r.body as Map)['hasNextPage'] as bool?`.
  final HasMoreOf? hasMoreOf;

  @override
  Map<String, String> query(String? cursor, int limit) => {
        pageParam: cursor ?? '$firstPage',
        sizeParam: '$limit',
      };

  @override
  PullPage toPage(
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      _page(records, cursor, records.length >= limit);

  @override
  PullPage toPageFromResponse(RestResponse response,
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      _page(records, cursor,
          hasMoreOf?.call(response) ?? records.length >= limit);

  PullPage _page(
      List<Map<String, dynamic>> records, String? cursor, bool hasMore) {
    final page = int.tryParse(cursor ?? '') ?? firstPage;
    // An empty page is always the last one, whatever the server claims.
    final more = hasMore && records.isNotEmpty;
    return PullPage(
      records: records,
      hasMore: more,
      nextCursor: more ? '${page + 1}' : null,
    );
  }
}

/// `?offset=0&limit=500`. Like [PagePagination], use it with
/// `PullMode.fullRefresh`, and mind the same server page-size caps.
class OffsetPagination extends PullPagination {
  const OffsetPagination({
    this.offsetParam = 'offset',
    this.sizeParam = 'limit',
    this.hasMoreOf,
  });

  final String offsetParam;
  final String sizeParam;
  final HasMoreOf? hasMoreOf;

  @override
  Map<String, String> query(String? cursor, int limit) => {
        offsetParam: cursor ?? '0',
        sizeParam: '$limit',
      };

  @override
  PullPage toPage(
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      _page(records, cursor, records.length >= limit);

  @override
  PullPage toPageFromResponse(RestResponse response,
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      _page(records, cursor,
          hasMoreOf?.call(response) ?? records.length >= limit);

  PullPage _page(
      List<Map<String, dynamic>> records, String? cursor, bool hasMore) {
    final offset = int.tryParse(cursor ?? '') ?? 0;
    final more = hasMore && records.isNotEmpty;
    return PullPage(
      records: records,
      hasMore: more,
      nextCursor: more ? '${offset + records.length}' : null,
    );
  }
}

/// `?updatedSince=<timestamp>&count=500`, for backends that can filter by
/// modification time. The cursor is the newest timestamp seen, so pulls
/// resume where they stopped. Use it with `PullMode.incremental`.
///
/// The server should return records sorted by modification time and filter
/// with `>=`, so records sharing the boundary timestamp are not skipped.
/// When more than a page of records can share one timestamp, prefer
/// [CursorPagination] with a server-side cursor.
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

/// For APIs that return the cursor of the next page in the response, such
/// as `{"data": [...], "nextCursor": "c2", "hasMore": true}`.
///
/// With `PullMode.incremental` the last cursor is stored and the next pull
/// resumes from it, which also suits "sync token" APIs.
class CursorPagination extends PullPagination {
  const CursorPagination({
    this.cursorParam = 'cursor',
    this.sizeParam = 'limit',
    this.nextCursorOf = defaultNextCursorOf,
    this.hasMoreOf = defaultHasMoreOf,
  });

  final String cursorParam;
  final String sizeParam;

  /// Reads the cursor of the next page; null or empty when there is none.
  final String? Function(RestResponse response) nextCursorOf;

  /// Reads whether another page exists. When it returns null, there is
  /// another page if the response had records and a new cursor.
  final HasMoreOf hasMoreOf;

  @override
  Map<String, String> query(String? cursor, int limit) => {
        if (cursor != null) cursorParam: cursor,
        sizeParam: '$limit',
      };

  /// Without the response there is no next cursor: the page is the last.
  @override
  PullPage toPage(
          List<Map<String, dynamic>> records, String? cursor, int limit) =>
      PullPage(records: records, nextCursor: cursor);

  @override
  PullPage toPageFromResponse(RestResponse response,
      List<Map<String, dynamic>> records, String? cursor, int limit) {
    final raw = nextCursorOf(response);
    final next = raw == null || raw.isEmpty ? null : raw;
    final hasMore = hasMoreOf(response) ??
        (next != null && next != cursor && records.isNotEmpty);
    return PullPage(
      records: records,
      hasMore: hasMore,
      nextCursor: next ?? cursor,
    );
  }

  static const _containers = [
    'meta',
    'pagination',
    'paging',
    'page',
    'response_metadata',
  ];

  /// Looks for `nextCursor`, `next_cursor`, `nextPageToken` or
  /// `next_page_token` in the body or in a `meta`, `pagination`, `paging`,
  /// `page` or `response_metadata` object.
  static String? defaultNextCursorOf(RestResponse response) =>
      _lookup(response.body, const [
        'nextCursor',
        'next_cursor',
        'nextPageToken',
        'next_page_token',
      ])?.toString();

  /// Looks for a boolean `hasMore`, `has_more`, `hasNext` or `has_next`
  /// the same way as [defaultNextCursorOf].
  static bool? defaultHasMoreOf(RestResponse response) {
    final value = _lookup(response.body,
        const ['hasMore', 'has_more', 'hasNext', 'has_next', 'hasNextPage']);
    return value is bool ? value : null;
  }

  static Object? _lookup(Object? body, List<String> keys) {
    if (body is! Map) return null;
    for (final key in keys) {
      final value = body[key];
      if (value != null) return value;
    }
    for (final container in _containers) {
      final nested = body[container];
      if (nested is! Map) continue;
      for (final key in keys) {
        final value = nested[key];
        if (value != null) return value;
      }
    }
    return null;
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
/// Responses map to outcomes as follows (see [classify]):
///
/// | response                         | outcome                  |
/// |----------------------------------|--------------------------|
/// | 2xx                              | success                  |
/// | [unauthorizedStatusCodes] (401)  | unauthorized             |
/// | [isAlreadyExists] on a create    | success, record adopted  |
/// | 409                              | conflict                 |
/// | 404 on a delete                  | success (already gone)   |
/// | 408, 425, 429, 5xx               | retry (honors Retry-After) |
/// | anything else (3xx, 4xx)         | rejected                 |
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
    this.unauthorizedStatusCodes = const {401},
    Object? Function(Object? body)? extractServerId,
    Map<String, dynamic>? Function(Object? body)? extractRecord,
    List<Map<String, dynamic>> Function(Object? body)? extractRecords,
    String? Function(RestResponse response)? applicationError,
    String Function(RestResponse response)? errorMessage,
    bool Function(RestResponse response)? isAlreadyExists,
  })  : createPath = createPath ?? ((_) => resource),
        updatePath = updatePath ?? ((r) => '$resource/${r.serverId}'),
        deletePath = deletePath ?? ((r) => '$resource/${r.serverId}'),
        pullPath = pullPath ?? resource,
        extractServerId = extractServerId ?? defaultExtractServerId,
        extractRecord = extractRecord ?? defaultExtractRecord,
        extractRecords = extractRecords ?? defaultExtractRecords,
        applicationError = applicationError ?? defaultApplicationError,
        errorMessage = errorMessage ?? defaultErrorMessage,
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

  /// Status codes meaning "the session expired": sync stops and reports
  /// `SyncPhase.authRequired` until the next successful run.
  ///
  /// 403 is not included by default because it usually means "not allowed
  /// for this record", which should fail that one operation instead of
  /// stopping sync. Use `{401, 403}` if your server answers 403 to an
  /// expired token.
  final Set<int> unauthorizedStatusCodes;

  final Object? Function(Object? body) extractServerId;
  final Map<String, dynamic>? Function(Object? body) extractRecord;
  final List<Map<String, dynamic>> Function(Object? body) extractRecords;

  /// Returns an error message when a 2xx response is actually a failure.
  final String? Function(RestResponse response) applicationError;

  /// Describes a failed response, for `SyncOperation.lastError`.
  final String Function(RestResponse response) errorMessage;

  /// True when the server refused a create because the record already
  /// exists (e.g. the first request succeeded but its response was lost).
  /// The record is then adopted if the response carries its id. Checked
  /// before a 409 is treated as a conflict.
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
    if (unauthorizedStatusCodes.contains(code)) {
      return PushOutcome.unauthorized(errorMessage(response));
    }

    final failure = response.isSuccessful
        ? applicationError(response)
        : errorMessage(response);
    if (failure == null) {
      return PushOutcome.success(
        serverId: SyncIds.normalize(extractServerId(response.body)),
        record: extractRecord(response.body),
      );
    }

    if (request.type == SyncOpType.create && isAlreadyExists(response)) {
      final id = SyncIds.normalize(extractServerId(response.body));
      if (id != null) {
        return PushOutcome.success(
            serverId: id, record: extractRecord(response.body));
      }
    }
    if (code == 409) {
      return PushOutcome.conflict(
          serverRecord: extractRecord(response.body), error: failure);
    }
    if (code == 404 && request.type == SyncOpType.delete) {
      // Already gone: the goal of the delete is reached.
      return const PushOutcome.success();
    }
    if (code == 408 || code == 425 || code == 429 || code >= 500) {
      return PushOutcome.retry(failure,
          retryAfter: parseRetryAfter(response.header('retry-after')));
    }
    return PushOutcome.rejected(failure);
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
    if (unauthorizedStatusCodes.contains(response.statusCode)) {
      throw SyncUnauthorizedException(errorMessage(response));
    }
    final failure = response.isSuccessful
        ? applicationError(response)
        : errorMessage(response);
    if (failure != null) throw SyncRejectedException(failure);
    return pagination.toPageFromResponse(
        response, extractRecords(response.body), request.cursor, request.limit);
  }

  /// Parses a `Retry-After` header: a number of seconds or an HTTP date.
  static Duration? parseRetryAfter(String? value, {DateTime? now}) {
    if (value == null) return null;
    final text = value.trim();
    final seconds = int.tryParse(text);
    if (seconds != null) return Duration(seconds: seconds < 0 ? 0 : seconds);
    final date = _parseHttpDate(text);
    if (date == null) return null;
    final delay = date.difference(now ?? DateTime.now());
    return delay.isNegative ? Duration.zero : delay;
  }

  static const _months = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec'
  ];

  /// `Sun, 06 Nov 1994 08:49:37 GMT` (RFC 9110 IMF-fixdate).
  static DateTime? _parseHttpDate(String text) {
    final match = RegExp(r'^[A-Za-z]{3}, (\d{1,2}) ([A-Za-z]{3}) (\d{4}) '
            r'(\d{2}):(\d{2}):(\d{2}) GMT$')
        .firstMatch(text);
    if (match == null) return null;
    final month = _months.indexOf(match[2]!) + 1;
    if (month == 0) return null;
    return DateTime.utc(int.parse(match[3]!), month, int.parse(match[1]!),
        int.parse(match[4]!), int.parse(match[5]!), int.parse(match[6]!));
  }

  /// A readable message from an error response: `message`, an RFC 7807
  /// `detail` or `title`, or `error`, followed by the validation `errors`.
  /// Falls back to the status code.
  static String defaultErrorMessage(RestResponse response) =>
      _describe(response.body) ?? 'HTTP ${response.statusCode}';

  static String? _describe(Object? body, [int depth = 0]) {
    if (body is String) {
      final text = body.trim();
      // Skip empty bodies and HTML error pages.
      if (text.isEmpty || text.startsWith('<')) return null;
      return text.length > 300 ? '${text.substring(0, 300)}…' : text;
    }
    if (body is! Map || depth > 2) return null;
    String? message;
    for (final key in const [
      'message',
      'Message',
      'detail',
      'Detail',
      'title',
      'Title',
      'error_description',
      'error',
      'Error',
    ]) {
      final value = body[key];
      message = value is Map
          ? _describe(value, depth + 1)
          : (value is String && value.trim().isNotEmpty ? value.trim() : null);
      if (message != null) break;
    }
    final errors = _formatErrors(body['errors'] ?? body['Errors']);
    if (message == null && errors == null) return null;
    return [if (message != null) message, if (errors != null) errors].join(' ');
  }

  /// `{"name": ["is required"]}` becomes `name: is required`.
  static String? _formatErrors(Object? errors) {
    if (errors == null) return null;
    String text(Object? value) =>
        value is List ? value.map(text).join(', ') : '$value';
    final out = switch (errors) {
      Map() =>
        errors.entries.map((e) => '${e.key}: ${text(e.value)}').join('; '),
      List() => errors.map(text).join('; '),
      _ => '$errors',
    };
    return out.isEmpty ? null : out;
  }

  /// Reads `data.id`, then `id`, or the body itself when the server answers
  /// a create with the bare numeric id.
  static Object? defaultExtractServerId(Object? body) {
    if (body is num) return body;
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

  static const _listKeys = [
    'data',
    'Data',
    'items',
    'Items',
    'results',
    'Results',
    'records',
    'Records',
    'rows',
    'value',
    'dataList',
  ];

  /// Finds the records of a pull response: the body itself when it is a
  /// list, or a list under `data`, `items`, `results`, `records`, `rows`,
  /// `value` or `dataList` (also one level down, as in
  /// `{"data": {"items": [...]}}`). A null body or list means no records.
  ///
  /// Throws a [FormatException] when there is no such list, rather than
  /// reading an unknown format as "no records" (which would make a full
  /// refresh delete every local row).
  static List<Map<String, dynamic>> defaultExtractRecords(Object? body) {
    final list = _findRecordList(body, 0);
    if (list == null) {
      throw FormatException(
          'No list of records found in the pull response. Pass '
          'extractRecords to RestRemoteAdapter to read this format.',
          body is Map ? '{${body.keys.join(', ')}}' : body.runtimeType);
    }
    return [
      for (final item in list)
        if (item is Map) Map<String, dynamic>.from(item),
    ];
  }

  static List<Object?>? _findRecordList(Object? body, int depth) {
    if (body == null) return const [];
    if (body is List) return body;
    if (body is! Map || depth > 1) return null;
    for (final key in _listKeys) {
      if (!body.containsKey(key)) continue;
      final value = body[key];
      if (value == null) return const [];
      if (value is List) return value;
      if (value is Map) {
        final nested = _findRecordList(value, depth + 1);
        if (nested != null) return nested;
      }
    }
    return null;
  }

  /// Treats `{"success": false}` or `{"status": false}` as an error.
  static String? defaultApplicationError(RestResponse response) {
    final body = response.body;
    if (body is! Map) return null;
    final success = body['success'] ?? body['Success'];
    final status = body['status'] ?? body['Status'];
    if (success == false || status == false) {
      return _describe(body) ?? 'Request rejected';
    }
    return null;
  }
}
