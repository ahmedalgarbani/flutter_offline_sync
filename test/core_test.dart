import 'dart:async';
import 'dart:math';

import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

SyncOperation _op(DateTime updatedAt, Map<String, dynamic> payload) =>
    SyncOperation(
      id: 'op-${updatedAt.millisecondsSinceEpoch}',
      entity: 'notes',
      localId: 'a',
      type: SyncOpType.update,
      payload: payload,
      createdAt: updatedAt,
      updatedAt: updatedAt,
    );

SyncConflict _conflict(
  Map<String, dynamic>? serverRecord,
  List<SyncOperation> pending,
) =>
    SyncConflict(
      entity: 'notes',
      localId: 'a',
      serverId: '1',
      serverRecord: serverRecord,
      pendingOperations: pending,
      source: ConflictSource.pull,
    );

// Named like the transport errors the classifier looks for.
class SocketException implements Exception {
  @override
  String toString() => 'SocketException: Connection refused';
}

class _Opaque implements Exception {
  _Opaque(this.text);
  final String text;
  @override
  String toString() => text;
}

void main() {
  group('SyncIds', () {
    test('uuid() returns distinct RFC 4122 v4 ids', () {
      final pattern = RegExp(
          r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');
      final ids = {for (var i = 0; i < 1000; i++) SyncIds.uuid()};
      expect(ids, hasLength(1000));
      expect(ids.every(pattern.hasMatch), isTrue);
    });

    test('normalize() turns "no id" values into null', () {
      expect(SyncIds.normalize(null), isNull);
      expect(SyncIds.normalize(''), isNull);
      expect(SyncIds.normalize('  '), isNull);
      expect(SyncIds.normalize(0), isNull);
      expect(SyncIds.normalize('0'), isNull);
      expect(SyncIds.normalize('null'), isNull);
      expect(SyncIds.normalize(42), '42');
      expect(SyncIds.normalize(' abc '), 'abc');
    });

    test('toJsonValue() keeps the type of the original value', () {
      expect(SyncIds.toJsonValue('42', 7), 42);
      expect(SyncIds.toJsonValue('42', '7'), '42');
      expect(SyncIds.toJsonValue('a-uuid', 7), 'a-uuid');
    });
  });

  group('RetryPolicy', () {
    test('delays grow exponentially up to maxDelay', () {
      const policy = RetryPolicy(
        baseDelay: Duration(seconds: 1),
        maxDelay: Duration(seconds: 10),
        jitter: 0,
      );
      expect([for (var a = 1; a <= 6; a++) policy.delayFor(a).inSeconds],
          [1, 2, 4, 8, 10, 10]);
      expect(policy.delayFor(1000), const Duration(seconds: 10));
    });

    test('jitter stays within bounds', () {
      const policy = RetryPolicy(baseDelay: Duration(seconds: 10), jitter: 0.2);
      final random = Random(1);
      for (var i = 0; i < 200; i++) {
        final ms = policy.delayFor(1, random).inMilliseconds;
        expect(ms, inInclusiveRange(8000, 12000));
      }
    });

    test('isExhausted() follows maxAttempts; null retries forever', () {
      const policy = RetryPolicy(maxAttempts: 3);
      expect(policy.isExhausted(2), isFalse);
      expect(policy.isExhausted(3), isTrue);
      expect(
          const RetryPolicy(maxAttempts: null).isExhausted(1 << 30), isFalse);
    });
  });

  group('SyncReference', () {
    test('reads plain, dotted and list paths', () {
      final json = {
        'customerId': 5,
        'payment': {'accountId': 'acc'},
        'items': [
          {'itemId': 1},
          {'itemId': null},
          {'other': 3},
          {'itemId': 2},
        ],
        'groups': [
          {
            'lines': [
              {'id': 'x'},
              {'id': 'y'},
            ]
          },
        ],
      };
      expect(SyncReference('customerId', 'c').read(json), [5]);
      expect(SyncReference('payment.accountId', 'a').read(json), ['acc']);
      expect(SyncReference('items[].itemId', 'i').read(json), [1, 2]);
      expect(SyncReference('groups[].lines[].id', 'l').read(json), ['x', 'y']);
      expect(SyncReference('missing.path', 'm').read(json), isEmpty);
      expect(SyncReference('customerId.deeper', 'm').read(json), isEmpty);
    });

    test('rewrites into a deep copy', () {
      final json = {
        'items': [
          {'itemId': 1},
          {'itemId': 2},
        ],
      };
      final out = SyncReference('items[].itemId', 'items')
          .rewrite(json, (v) => 'server-$v');
      expect(out, {
        'items': [
          {'itemId': 'server-1'},
          {'itemId': 'server-2'},
        ],
      });
      expect(json['items']!.first['itemId'], 1);
    });

    test('rejects empty paths and segments', () {
      expect(() => SyncReference('', 'x'), throwsArgumentError);
      expect(() => SyncReference('a..b', 'x'), throwsArgumentError);
      expect(() => SyncReference('[].id', 'x'), throwsArgumentError);
    });

    test('deepCopyJson() copies nested maps and lists', () {
      final original = <String, dynamic>{
        'list': [
          {'a': 1},
        ],
        'map': {'b': 2},
      };
      final copy = deepCopyJson(original);
      ((copy['list'] as List).first as Map)['a'] = 9;
      (copy['map'] as Map)['b'] = 9;
      expect(original, {
        'list': [
          {'a': 1},
        ],
        'map': {'b': 2},
      });
    });
  });

  group('conflict strategies', () {
    final t1 = DateTime(2024, 1, 1, 10);
    final t2 = DateTime(2024, 1, 1, 11);
    final local = [
      _op(t1, {'x': 1})
    ];

    test('keepLocal and serverWins', () async {
      expect(
          await resolverFor(ConflictStrategy.keepLocal)(_conflict({}, local)),
          isA<KeepLocal>());
      expect(
          await resolverFor(ConflictStrategy.serverWins)(_conflict({}, local)),
          isA<TakeServer>());
    });

    test('lastWriteWins compares timestamps', () async {
      final resolve = resolverFor(ConflictStrategy.lastWriteWins,
          updatedAtOf: (r) => r['at'] as DateTime?);
      expect(await resolve(_conflict({'at': t2}, local)), isA<TakeServer>());
      expect(await resolve(_conflict({'at': t1}, [_op(t2, {})])),
          isA<KeepLocal>());
      // No server timestamp: the local change is kept.
      expect(await resolve(_conflict({}, local)), isA<KeepLocal>());
      expect(await resolve(_conflict(null, local)), isA<KeepLocal>());
    });

    test('localPayload merges pending payloads oldest first', () {
      final conflict = _conflict({}, [
        _op(t1, {'a': 1, 'b': 1}),
        _op(t2, {'b': 2}),
      ]);
      expect(conflict.localPayload, {'a': 1, 'b': 2});
    });
  });

  group('SyncStatus', () {
    test('equal values are equal, so listeners are not re-notified', () {
      final at = DateTime(2024);
      expect(
        SyncStatus(pendingCount: 2, lastSyncedAt: at),
        SyncStatus(pendingCount: 2, lastSyncedAt: at),
      );
      expect(
        SyncStatus(pendingCount: 2, lastSyncedAt: at).hashCode,
        SyncStatus(pendingCount: 2, lastSyncedAt: at).hashCode,
      );
      expect(const SyncStatus(pendingCount: 1),
          isNot(const SyncStatus(pendingCount: 2)));
    });

    test('copyWith can clear optional fields', () {
      final status = SyncStatus(
        currentEntity: 'notes',
        lastError: 'x',
        nextRetryAt: DateTime(2024),
      ).copyWith(
        clearCurrentEntity: true,
        clearLastError: true,
        clearNextRetryAt: true,
      );
      expect(status.currentEntity, isNull);
      expect(status.lastError, isNull);
      expect(status.nextRetryAt, isNull);
    });

    test('isSyncing and isUpToDate', () {
      expect(const SyncStatus(phase: SyncPhase.pulling).isSyncing, isTrue);
      expect(const SyncStatus(phase: SyncPhase.offline).isSyncing, isFalse);
      expect(const SyncStatus().isUpToDate, isTrue);
      expect(const SyncStatus(failedCount: 1).isUpToDate, isFalse);
    });
  });

  group('defaultClassifyError', () {
    test('maps the package exceptions', () {
      expect(defaultClassifyError(const SyncNetworkException()),
          isA<PushNetworkError>());
      expect(defaultClassifyError(const SyncUnauthorizedException()),
          isA<PushUnauthorized>());
      expect(defaultClassifyError(const SyncRejectedException('bad')),
          isA<PushRejected>());
    });

    test('recognizes transport failures as network errors', () {
      for (final error in <Object>[
        TimeoutException('slow'),
        SocketException(),
        // Release web builds minify type names; the message survives.
        _Opaque('ClientException: XMLHttpRequest error.'),
        // package:http wraps socket errors in a private subclass.
        _Opaque('ClientException with SocketException: Failed host lookup'),
        _Opaque('HandshakeException: Handshake error in client'),
        _Opaque('DioException [connection error]: The connection errored'),
        _Opaque('DioException [receive timeout]: took longer than 30s'),
      ]) {
        expect(isNetworkError(error), isTrue, reason: '$error');
        expect(defaultClassifyError(error), isA<PushNetworkError>(),
            reason: '$error');
      }
    });

    test('treats anything else as a temporary failure', () {
      for (final error in <Object>[
        StateError('bug'),
        const FormatException('bad json'),
        _Opaque('DioException [bad response]: 500'),
        _Opaque('MyClientExceptionHandler failed'),
      ]) {
        expect(isNetworkError(error), isFalse, reason: '$error');
        expect(defaultClassifyError(error), isA<PushRetry>(), reason: '$error');
      }
    });
  });

  test('SyncOperation survives a row round trip', () {
    final op = SyncOperation(
      id: 'id',
      seq: 3,
      entity: 'bills',
      localId: '1',
      type: SyncOpType.delete,
      payload: const {'a': 1},
      status: SyncOpStatus.failed,
      attempts: 2,
      force: true,
      createdAt: DateTime(2024, 1, 1),
      updatedAt: DateTime(2024, 1, 2),
      nextAttemptAt: DateTime(2024, 1, 3),
      lastError: 'e',
    );
    final back = SyncOperation.fromRow({...op.toRow(), 'seq': 3});
    expect(back.toRow(), op.toRow());
    expect(back.seq, 3);
  });

  group('connectivity', () {
    test('StreamConnectivity reports changes once and survives errors',
        () async {
      final source = StreamController<bool>();
      final connectivity =
          StreamConnectivity(initial: true, changes: source.stream);
      final seen = <bool>[];
      connectivity.onChanged.listen(seen.add);
      source
        ..add(true)
        ..add(false)
        ..addError('plugin failure')
        ..add(false)
        ..add(true);
      await Future<void>.delayed(Duration.zero);
      expect(seen, [false, true]);
      expect(connectivity.isOnline, isTrue);
      await connectivity.dispose();
      await source.close();
    });

    test('ManualConnectivity notifies on change only', () async {
      final connectivity = ManualConnectivity(false);
      final seen = <bool>[];
      connectivity.onChanged.listen(seen.add);
      connectivity
        ..online = false
        ..online = true
        ..online = true;
      await Future<void>.delayed(Duration.zero);
      expect(seen, [true]);
      await connectivity.dispose();
      connectivity.online = false; // ignored after dispose
      expect(connectivity.isOnline, isFalse);
    });
  });
}
