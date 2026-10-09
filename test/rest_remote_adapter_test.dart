import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

PushRequest _request(
  SyncOpType type, {
  String? serverId,
  Map<String, dynamic> payload = const {'name': 'x'},
  bool force = false,
}) =>
    PushRequest(
      operation: SyncOperation(
        id: 'op-1',
        entity: 'customers',
        localId: 'a',
        type: type,
        payload: payload,
        force: force,
        createdAt: DateTime(2024),
        updatedAt: DateTime(2024),
      ),
      payload: payload,
      serverId: serverId,
    );

/// An adapter whose transport records requests and answers with [respond].
(RestRemoteAdapter, List<RestRequest>) _adapter(
  RestResponse Function(RestRequest request) respond, {
  String? forceHeader,
  String updateMethod = 'PUT',
  bool sendDeleteBody = false,
  Set<int> unauthorizedStatusCodes = const {401},
  bool Function(RestResponse response)? isAlreadyExists,
  PullPagination pagination = const PagePagination(),
}) {
  final sent = <RestRequest>[];
  final adapter = RestRemoteAdapter(
    resource: 'customers',
    transport: (request) async {
      sent.add(request);
      return respond(request);
    },
    forceHeader: forceHeader,
    updateMethod: updateMethod,
    sendDeleteBody: sendDeleteBody,
    unauthorizedStatusCodes: unauthorizedStatusCodes,
    isAlreadyExists: isAlreadyExists,
    pagination: pagination,
    pullQuery: const {'scope': 'all'},
  );
  return (adapter, sent);
}

Future<PushOutcome> _classify(
  RestResponse response, {
  SyncOpType type = SyncOpType.update,
  Set<int> unauthorizedStatusCodes = const {401},
  bool Function(RestResponse response)? isAlreadyExists,
}) {
  final (adapter, _) = _adapter((_) => response,
      unauthorizedStatusCodes: unauthorizedStatusCodes,
      isAlreadyExists: isAlreadyExists);
  return adapter.push(_request(type, serverId: '5'));
}

void main() {
  group('requests', () {
    test('create, update and delete map to POST, PUT and DELETE', () async {
      final (adapter, sent) =
          _adapter((_) => const RestResponse(statusCode: 200, body: {'id': 9}));
      await adapter.push(_request(SyncOpType.create));
      await adapter.push(_request(SyncOpType.update, serverId: '9'));
      await adapter.push(_request(SyncOpType.delete, serverId: '9'));

      expect(sent.map((r) => '${r.method} ${r.path}'),
          ['POST customers', 'PUT customers/9', 'DELETE customers/9']);
      expect(sent[0].body, {'name': 'x'});
      expect(sent[2].body, isNull);
      for (final request in sent) {
        expect(request.headers['Idempotency-Key'], 'op-1');
      }
    });

    test('options: update method, delete body, force header', () async {
      final (adapter, sent) = _adapter(
        (_) => const RestResponse(statusCode: 204),
        updateMethod: 'PATCH',
        sendDeleteBody: true,
        forceHeader: 'X-Force',
      );
      await adapter.push(_request(SyncOpType.update, serverId: '9'));
      await adapter
          .push(_request(SyncOpType.delete, serverId: '9', force: true));
      expect(sent[0].method, 'PATCH');
      expect(sent[0].headers.containsKey('X-Force'), isFalse);
      expect(sent[1].body, {'name': 'x'});
      expect(sent[1].headers['X-Force'], 'true');
    });
  });

  group('responses', () {
    test('2xx is a success carrying the server id and record', () async {
      final outcome = await _classify(
          const RestResponse(statusCode: 201, body: {
            'data': {'id': 12, 'name': 'x'},
          }),
          type: SyncOpType.create) as PushSuccess;
      expect(outcome.serverId, '12');
      expect(outcome.record, {'id': 12, 'name': 'x'});

      final bare = await _classify(const RestResponse(statusCode: 200, body: 7),
          type: SyncOpType.create) as PushSuccess;
      expect(bare.serverId, '7');

      final top = await _classify(
          const RestResponse(statusCode: 200, body: {'Id': 'abc'}),
          type: SyncOpType.create) as PushSuccess;
      expect(top.serverId, 'abc');
    });

    test('401 means the session expired', () async {
      expect(await _classify(const RestResponse(statusCode: 401)),
          isA<PushUnauthorized>());
    });

    test('403 fails the operation unless configured as unauthorized', () async {
      expect(
          await _classify(const RestResponse(
              statusCode: 403, body: {'message': 'Not your branch'})),
          isA<PushRejected>()
              .having((o) => o.error, 'error', 'Not your branch'));
      expect(
          await _classify(const RestResponse(statusCode: 403),
              unauthorizedStatusCodes: {401, 403}),
          isA<PushUnauthorized>());
    });

    test('3xx is not a success', () async {
      expect(await _classify(const RestResponse(statusCode: 302)),
          isA<PushRejected>());
    });

    test('409 is a conflict carrying the server copy', () async {
      final outcome =
          await _classify(const RestResponse(statusCode: 409, body: {
        'message': 'Changed elsewhere',
        'data': {'id': 5, 'name': 'server'},
      })) as PushConflict;
      expect(outcome.serverRecord, {'id': 5, 'name': 'server'});
      expect(outcome.error, 'Changed elsewhere');
    });

    test('an "already exists" create adopts the existing record', () async {
      bool exists(RestResponse r) =>
          r.statusCode == 409 || (r.body as Map?)?['code'] == 'DUPLICATE';
      final conflict = await _classify(
          const RestResponse(statusCode: 409, body: {'id': 77}),
          type: SyncOpType.create,
          isAlreadyExists: exists) as PushSuccess;
      expect(conflict.serverId, '77');

      final enveloped = await _classify(
          const RestResponse(
              statusCode: 200,
              body: {'success': false, 'code': 'DUPLICATE', 'id': 78}),
          type: SyncOpType.create,
          isAlreadyExists: exists) as PushSuccess;
      expect(enveloped.serverId, '78');

      // Without an id to adopt, the create is rejected.
      expect(
          await _classify(const RestResponse(statusCode: 400, body: {}),
              type: SyncOpType.create, isAlreadyExists: (_) => true),
          isA<PushRejected>());
    });

    test('404 completes a delete but rejects an update', () async {
      expect(
          await _classify(const RestResponse(statusCode: 404),
              type: SyncOpType.delete),
          isA<PushSuccess>());
      expect(await _classify(const RestResponse(statusCode: 404)),
          isA<PushRejected>());
    });

    test('408, 425, 429 and 5xx are retried, honoring Retry-After', () async {
      for (final code in [408, 425, 429, 500, 503]) {
        expect(
            await _classify(RestResponse(statusCode: code)), isA<PushRetry>(),
            reason: '$code');
      }
      final seconds = await _classify(const RestResponse(
          statusCode: 429, headers: {'RETRY-AFTER': '30'})) as PushRetry;
      expect(seconds.retryAfter, const Duration(seconds: 30));
    });

    test('Retry-After accepts seconds and HTTP dates', () {
      final now = DateTime.utc(2015, 10, 21, 7, 27, 30);
      expect(
          RestRemoteAdapter.parseRetryAfter('120'), const Duration(minutes: 2));
      expect(
          RestRemoteAdapter.parseRetryAfter('Wed, 21 Oct 2015 07:28:00 GMT',
              now: now),
          const Duration(seconds: 30));
      expect(
          RestRemoteAdapter.parseRetryAfter('Wed, 21 Oct 2015 07:00:00 GMT',
              now: now),
          Duration.zero);
      expect(RestRemoteAdapter.parseRetryAfter('soon'), isNull);
      expect(RestRemoteAdapter.parseRetryAfter(null), isNull);
    });

    test('a 200 envelope with success: false is rejected', () async {
      expect(
          await _classify(const RestResponse(statusCode: 200, body: {
            'success': false,
            'message': 'Phone is invalid',
          })),
          isA<PushRejected>()
              .having((o) => o.error, 'error', 'Phone is invalid'));
    });
  });

  group('error messages', () {
    String message(Object? body, [int code = 400]) =>
        RestRemoteAdapter.defaultErrorMessage(
            RestResponse(statusCode: code, body: body));

    test('reads message and validation errors', () {
      expect(
          message({
            'message': 'Invalid data',
            'errors': {
              'Name': ['is required', 'is too short'],
              'Phone': 'is invalid',
            },
          }),
          'Invalid data Name: is required, is too short; Phone: is invalid');
      expect(
          message({
            'errors': ['a', 'b']
          }),
          'a; b');
    });

    test('reads RFC 7807 problem details', () {
      expect(
          message({
            'type': 'https://tools.ietf.org/html/rfc9110#section-15.5.1',
            'title': 'One or more validation errors occurred.',
            'status': 400,
            'errors': {
              'Email': ['The Email field is required.'],
            },
          }),
          'One or more validation errors occurred. '
          'Email: The Email field is required.');
      expect(message({'title': 'Not Found', 'detail': 'No customer 5'}),
          'No customer 5');
    });

    test('reads nested error objects and plain text', () {
      expect(
          message({
            'error': {'message': 'quota exceeded'}
          }),
          'quota exceeded');
      expect(message('Bad things happened'), 'Bad things happened');
    });

    test('falls back to the status code', () {
      expect(message(null, 502), 'HTTP 502');
      expect(message('<html><body>502 Bad Gateway</body></html>', 502),
          'HTTP 502');
      expect(message({'unrelated': true}, 400), 'HTTP 400');
    });
  });

  group('pull', () {
    test('sends pagination and extra query parameters', () async {
      final (adapter, sent) =
          _adapter((_) => const RestResponse(statusCode: 200, body: []));
      await adapter
          .pull(const PullRequest(entity: 'c', cursor: '3', limit: 50));
      expect(sent.single.method, 'GET');
      expect(sent.single.path, 'customers');
      expect(sent.single.query, {'scope': 'all', 'page': '3', 'count': '50'});
    });

    test('finds records in common response shapes', () {
      List<Map<String, dynamic>> extract(Object? body) =>
          RestRemoteAdapter.defaultExtractRecords(body);
      const record = {'id': 1};
      expect(extract([record]), [record]);
      expect(
          extract({
            'data': [record]
          }),
          [record]);
      expect(
          extract({
            'Items': [record],
            'total': 1
          }),
          [record]);
      expect(
          extract({
            'value': [record]
          }),
          [record]);
      expect(
          extract({
            'data': {
              'results': [record],
              'count': 1,
            },
          }),
          [record]);
      expect(extract({'data': null}), isEmpty);
      expect(extract(null), isEmpty);
    });

    test('an unknown response shape is an error, not an empty page', () {
      expect(() => RestRemoteAdapter.defaultExtractRecords({'payload': []}),
          throwsFormatException);
      expect(
          () => RestRemoteAdapter.defaultExtractRecords({
                'data': {'id': 1},
              }),
          throwsFormatException);
    });

    test('maps failures to the engine exceptions', () async {
      final (unauthorized, _) =
          _adapter((_) => const RestResponse(statusCode: 401));
      await expectLater(
          unauthorized
              .pull(const PullRequest(entity: 'c', cursor: null, limit: 5)),
          throwsA(isA<SyncUnauthorizedException>()));

      final (failing, _) = _adapter((_) => const RestResponse(
          statusCode: 200, body: {'success': false, 'message': 'Disabled'}));
      await expectLater(
          failing.pull(const PullRequest(entity: 'c', cursor: null, limit: 5)),
          throwsA(isA<SyncRejectedException>()
              .having((e) => e.message, 'message', 'Disabled')));
    });
  });

  group('pagination', () {
    const response = RestResponse(statusCode: 200);
    final full = List.generate(2, (i) => <String, dynamic>{'id': i});

    test('PagePagination: a full page means more', () {
      const pagination = PagePagination(pageParam: 'p', sizeParam: 's');
      expect(pagination.query(null, 2), {'p': '1', 's': '2'});
      final page = pagination.toPageFromResponse(response, full, '4', 2);
      expect(page.hasMore, isTrue);
      expect(page.nextCursor, '5');
      expect(
          pagination.toPageFromResponse(response, [full.first], '4', 2).hasMore,
          isFalse);
    });

    test('PagePagination: hasMoreOf overrides the guess, not an empty page',
        () {
      final pagination = PagePagination(
          hasMoreOf: (r) => (r.body as Map?)?['hasNext'] as bool?);
      const more = RestResponse(statusCode: 200, body: {'hasNext': true});
      // The server capped the page below the requested size.
      expect(
          pagination.toPageFromResponse(more, [full.first], null, 2).nextCursor,
          '2');
      expect(pagination.toPageFromResponse(more, const [], null, 2).hasMore,
          isFalse);
    });

    test('OffsetPagination advances by the records received', () {
      const pagination = OffsetPagination();
      expect(pagination.query(null, 2), {'offset': '0', 'limit': '2'});
      final page = pagination.toPageFromResponse(response, full, '10', 2);
      expect(page.nextCursor, '12');
      expect(page.hasMore, isTrue);
    });

    test('UpdatedSincePagination keeps the newest timestamp', () {
      final pagination =
          UpdatedSincePagination(updatedAtOf: (r) => r['at'] as String?);
      expect(pagination.query(null, 10), {'count': '10'});
      expect(pagination.query('2024-01-01T00:00:00Z', 10),
          {'updatedSince': '2024-01-01T00:00:00Z', 'count': '10'});
      final page = pagination.toPage([
        {'at': '2024-01-03T00:00:00Z'},
        {'at': '2024-01-02T00:00:00Z'},
        {'at': null},
      ], '2024-01-01T00:00:00Z', 10);
      expect(page.nextCursor, '2024-01-03T00:00:00Z');
      expect(page.hasMore, isFalse);
    });

    test('CursorPagination reads the next cursor from the response', () {
      const pagination = CursorPagination();
      expect(pagination.query(null, 5), {'limit': '5'});
      expect(pagination.query('c1', 5), {'cursor': 'c1', 'limit': '5'});

      PullPage page(Object? body, [String? cursor = 'c1']) =>
          pagination.toPageFromResponse(
              RestResponse(statusCode: 200, body: body), full, cursor, 5);

      expect(page({'nextCursor': 'c2'}).nextCursor, 'c2');
      expect(page({'nextCursor': 'c2'}).hasMore, isTrue);
      expect(
          page({
            'meta': {'next_cursor': 'c3'},
          }).nextCursor,
          'c3');
      // Slack-style: an empty cursor ends the pull; the last one is kept.
      final last = page({
        'response_metadata': {'next_cursor': ''},
      });
      expect(last.hasMore, isFalse);
      expect(last.nextCursor, 'c1');
      // An explicit flag wins.
      final flagged = page({'nextCursor': 'c9', 'has_more': false});
      expect(flagged.hasMore, isFalse);
      expect(flagged.nextCursor, 'c9');
    });
  });

  test('a 403 on one record does not stop the others', () async {
    final pushed = <String>[];
    final sync = SyncEngine(
      store: InMemorySyncStore(),
      config: manualConfig,
      entities: [
        SyncEntityConfig(
          name: 'notes',
          pullMode: PullMode.none,
          remote: RestRemoteAdapter(
            resource: 'notes',
            transport: (request) async {
              final body = request.body! as Map;
              if (body['text'] == 'forbidden') {
                return const RestResponse(
                    statusCode: 403, body: {'message': 'Not allowed'});
              }
              pushed.add(body['text'] as String);
              return RestResponse(statusCode: 201, body: {'id': pushed.length});
            },
          ),
          local: FakeLocalDb().adapter('notes'),
        ),
      ],
    );
    addTearDown(sync.dispose);
    await sync.recordCreate('notes', 'a', {'text': 'forbidden'});
    await sync.recordCreate('notes', 'b', {'text': 'ok'});
    final result = await sync.syncNow();

    expect(pushed, ['ok']);
    expect(result.failed, 1);
    expect(sync.status.value.phase, SyncPhase.idle);
    expect((await sync.failedOperations()).single.lastError, 'Not allowed');
  });
}
