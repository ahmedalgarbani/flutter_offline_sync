import 'dart:async';

import '../core/operation.dart';
import '../core/references.dart';
import 'sync_store.dart';

/// Non-persistent [SyncStore] for tests and prototypes.
///
/// Behaves like a database with a single connection: transactions run one
/// at a time, roll back on error, and calls made outside a transaction wait
/// for the running one to finish.
class InMemorySyncStore implements SyncStore {
  final Map<String, SyncOperation> _ops = {};
  final Map<String, String?> _idMap = {};
  final Map<String, String> _meta = {};
  int _seq = 0;

  /// Completes when the last queued call has finished; null when idle.
  Future<void>? _tail;

  static final Object _txKey = Object();

  String _key(String entity, String id) => '$entity\u0000$id';

  bool get _inTransaction => identical(Zone.current[_txKey], this);

  /// Runs [action] after every call queued before it. Calls made from
  /// inside a transaction run directly: they belong to it.
  Future<T> _serialized<T>(FutureOr<T> Function() action) {
    if (_inTransaction) return Future.sync(action);
    Future<T> run() =>
        Future.sync(() => runZoned(action, zoneValues: {_txKey: this}));
    // An idle store runs the call at once. Nothing is chained onto a future
    // created earlier, possibly in another zone (such as a test's setUp).
    final previous = _tail;
    final result = previous == null ? run() : previous.then((_) => run());
    final tail = result.then<void>((_) {}, onError: (Object _) {});
    _tail = tail;
    tail.whenComplete(() {
      if (identical(_tail, tail)) _tail = null;
    });
    return result;
  }

  @override
  Future<void> init() async {}

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    if (_inTransaction) return action();
    return _serialized(() async {
      final ops = Map.of(_ops);
      final idMap = Map.of(_idMap);
      final meta = Map.of(_meta);
      final seq = _seq;
      try {
        return await action();
      } catch (_) {
        _ops
          ..clear()
          ..addAll(ops);
        _idMap
          ..clear()
          ..addAll(idMap);
        _meta
          ..clear()
          ..addAll(meta);
        _seq = seq;
        rethrow;
      }
    });
  }

  List<SyncOperation> _sorted(Iterable<SyncOperation> ops) =>
      ops.toList()..sort((a, b) => a.seq!.compareTo(b.seq!));

  /// Stored operations never share mutable payload maps with callers.
  SyncOperation _detached(SyncOperation operation, int? seq) =>
      operation.copyWith(seq: seq, payload: deepCopyJson(operation.payload));

  @override
  Future<SyncOperation> insertOperation(SyncOperation operation) =>
      _serialized(() {
        final stored = _detached(operation, ++_seq);
        _ops[stored.id] = stored;
        return stored;
      });

  @override
  Future<void> updateOperation(SyncOperation operation) => _serialized(() {
        final existing = _ops[operation.id];
        if (existing == null) return;
        _ops[operation.id] = _detached(operation, existing.seq);
      });

  @override
  Future<void> deleteOperation(String id) => _serialized(() {
        _ops.remove(id);
      });

  @override
  Future<SyncOperation?> operationById(String id) =>
      _serialized(() => _ops[id]);

  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) =>
      _serialized(() => _sorted(_ops.values
          .where((op) => statuses == null || statuses.contains(op.status))));

  @override
  Future<List<SyncOperation>> operationsFor(String entity, String localId) =>
      _serialized(() => _sorted(_ops.values
          .where((op) => op.entity == entity && op.localId == localId)));

  @override
  Future<Map<SyncOpStatus, int>> countByStatus() => _serialized(() {
        final counts = {for (final s in SyncOpStatus.values) s: 0};
        for (final op in _ops.values) {
          counts[op.status] = counts[op.status]! + 1;
        }
        return counts;
      });

  @override
  Future<void> resetInFlight() => _serialized(() {
        for (final op in _ops.values.toList()) {
          if (op.status == SyncOpStatus.inFlight) {
            _ops[op.id] = op.copyWith(status: SyncOpStatus.pending);
          }
        }
      });

  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) =>
      _serialized(() {
        final key = _key(entity, localId);
        if (!_idMap.containsKey(key)) return null;
        return IdMapping(
            entity: entity, localId: localId, serverId: _idMap[key]);
      });

  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) =>
      _serialized(() {
        final prefix = '$entity\u0000';
        for (final entry in _idMap.entries) {
          if (entry.value == serverId && entry.key.startsWith(prefix)) {
            return IdMapping(
              entity: entity,
              localId: entry.key.substring(prefix.length),
              serverId: serverId,
            );
          }
        }
        return null;
      });

  @override
  Future<void> putMapping(String entity, String localId, String? serverId) =>
      _serialized(() {
        _idMap[_key(entity, localId)] = serverId;
      });

  @override
  Future<void> deleteMapping(String entity, String localId) => _serialized(() {
        _idMap.remove(_key(entity, localId));
      });

  @override
  Future<String?> getMeta(String key) => _serialized(() => _meta[key]);

  @override
  Future<void> setMeta(String key, String? value) => _serialized(() {
        if (value == null) {
          _meta.remove(key);
        } else {
          _meta[key] = value;
        }
      });

  @override
  Future<void> clear() => _serialized(() {
        _ops.clear();
        _idMap.clear();
        _meta.clear();
      });
}
