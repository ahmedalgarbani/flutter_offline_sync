import 'dart:async';
import 'dart:convert';

import '../core/operation.dart';
import '../core/references.dart';
import 'sync_store.dart';

part 'key_value_sync_store.dart';

/// Non-persistent [SyncStore] for tests and prototypes.
///
/// Behaves like a database with a single connection: transactions run one
/// at a time, roll back on error, and calls made outside a transaction wait
/// for the running one to finish. Payloads are stored as JSON, as
/// `SqlSyncStore` does, so data that a database could not store (a
/// `DateTime`, for example) fails here too instead of only in production.
class InMemorySyncStore implements SyncStore {
  final Map<String, SyncOperation> _ops = {};
  final Map<String, String?> _idMap = {};
  final Map<String, String> _meta = {};
  int _seq = 0;

  /// Completes when the last queued call has finished; null when idle.
  Future<void>? _tail;

  /// Set when the transaction in progress changed something.
  bool _dirty = false;

  static final Object _txKey = Object();

  /// Whether committed changes are saved through [_persist].
  bool get _persistent => false;

  /// Saves the committed state. Runs inside the store's lock; throwing
  /// rolls the change back.
  Future<void> _persist() async {}

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

  /// Runs a change: inside the open transaction, or as its own one when the
  /// store persists (so a failed save rolls it back).
  Future<T> _write<T>(T Function() change) {
    if (_inTransaction) {
      _dirty = true;
      return Future.sync(change);
    }
    if (!_persistent) return _serialized(change);
    return transaction(() async {
      _dirty = true;
      return change();
    });
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
      _dirty = false;
      try {
        final result = await action();
        if (_dirty && _persistent) await _persist();
        return result;
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
      } finally {
        _dirty = false;
      }
    });
  }

  List<SyncOperation> _sorted(Iterable<SyncOperation> ops) =>
      (ops.toList()..sort((a, b) => a.seq!.compareTo(b.seq!)))
          .map(_copy)
          .toList();

  /// Stored operations never share mutable payload maps with callers, in
  /// either direction.
  SyncOperation _copy(SyncOperation operation) =>
      operation.copyWith(payload: deepCopyJson(operation.payload));

  /// The stored form: the payload goes through JSON, as in a database.
  SyncOperation _stored(SyncOperation operation, int? seq) =>
      operation.copyWith(
          seq: seq,
          payload: Map<String, dynamic>.from(
              jsonDecode(jsonEncode(operation.payload)) as Map));

  @override
  Future<SyncOperation> insertOperation(SyncOperation operation) => _write(() {
        final stored = _stored(operation, _seq + 1);
        _seq++;
        _ops[stored.id] = stored;
        return _copy(stored);
      });

  @override
  Future<void> updateOperation(SyncOperation operation) => _write(() {
        final existing = _ops[operation.id];
        if (existing == null) return;
        _ops[operation.id] = _stored(operation, existing.seq);
      });

  @override
  Future<void> deleteOperation(String id) => _write(() {
        _ops.remove(id);
      });

  @override
  Future<SyncOperation?> operationById(String id) => _serialized(() {
        final op = _ops[id];
        return op == null ? null : _copy(op);
      });

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
  Future<void> resetInFlight() => _write(() {
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
      _write(() {
        _idMap[_key(entity, localId)] = serverId;
      });

  @override
  Future<void> deleteMapping(String entity, String localId) => _write(() {
        _idMap.remove(_key(entity, localId));
      });

  @override
  Future<String?> getMeta(String key) => _serialized(() => _meta[key]);

  @override
  Future<void> setMeta(String key, String? value) => _write(() {
        if (value == null) {
          _meta.remove(key);
        } else {
          _meta[key] = value;
        }
      });

  @override
  Future<void> clear() => _write(() {
        _ops.clear();
        _idMap.clear();
        _meta.clear();
      });

  // Snapshots (KeyValueSyncStore) -------------------------------------------

  static const _snapshotVersion = 1;

  Map<String, Object?> _snapshot() => {
        'version': _snapshotVersion,
        'seq': _seq,
        'ops': [
          for (final op in _ops.values) {...op.toRow(), 'seq': op.seq},
        ],
        'ids': [
          for (final entry in _idMap.entries)
            [
              entry.key.substring(0, entry.key.indexOf('\u0000')),
              entry.key.substring(entry.key.indexOf('\u0000') + 1),
              entry.value,
            ],
        ],
        'meta': _meta,
      };

  void _restore(Map<String, Object?> snapshot) {
    final version = snapshot['version'];
    if (version != _snapshotVersion) {
      throw FormatException('Unsupported sync state version: $version');
    }
    final ops = <String, SyncOperation>{};
    for (final row in snapshot['ops']! as List) {
      final op = SyncOperation.fromRow(Map<String, Object?>.from(row as Map));
      ops[op.id] = op;
    }
    final ids = <String, String?>{};
    for (final entry in snapshot['ids']! as List) {
      final parts = entry as List;
      ids[_key(parts[0] as String, parts[1] as String)] = parts[2] as String?;
    }
    final meta = Map<String, String>.from(snapshot['meta']! as Map);
    _ops
      ..clear()
      ..addAll(ops);
    _idMap
      ..clear()
      ..addAll(ids);
    _meta
      ..clear()
      ..addAll(meta);
    _seq = (snapshot['seq']! as num).toInt();
  }
}
