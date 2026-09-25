import 'dart:async';

import '../core/operation.dart';
import 'sync_store.dart';

/// Non-persistent [SyncStore] for tests and prototypes. Transactions roll
/// back on error.
class InMemorySyncStore implements SyncStore {
  final Map<String, SyncOperation> _ops = {};
  final Map<String, String?> _idMap = {};
  final Map<String, String> _meta = {};
  int _seq = 0;

  static final Object _txKey = Object();

  String _key(String entity, String id) => '$entity\u0000$id';

  @override
  Future<void> init() async {}

  @override
  Future<T> transaction<T>(Future<T> Function() action) async {
    if (Zone.current[_txKey] == true) return action();
    final ops = Map.of(_ops);
    final idMap = Map.of(_idMap);
    final meta = Map.of(_meta);
    final seq = _seq;
    try {
      return await runZoned(action, zoneValues: {_txKey: true});
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
  }

  List<SyncOperation> _sorted(Iterable<SyncOperation> ops) =>
      ops.toList()..sort((a, b) => a.seq!.compareTo(b.seq!));

  @override
  Future<SyncOperation> insertOperation(SyncOperation operation) async {
    final stored = operation.copyWith(seq: ++_seq);
    _ops[stored.id] = stored;
    return stored;
  }

  @override
  Future<void> updateOperation(SyncOperation operation) async {
    final existing = _ops[operation.id];
    if (existing == null) return;
    _ops[operation.id] = operation.copyWith(seq: existing.seq);
  }

  @override
  Future<void> deleteOperation(String id) async => _ops.remove(id);

  @override
  Future<SyncOperation?> operationById(String id) async => _ops[id];

  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) async =>
      _sorted(_ops.values
          .where((op) => statuses == null || statuses.contains(op.status)));

  @override
  Future<List<SyncOperation>> operationsFor(
          String entity, String localId) async =>
      _sorted(_ops.values
          .where((op) => op.entity == entity && op.localId == localId));

  @override
  Future<Map<SyncOpStatus, int>> countByStatus() async {
    final counts = {for (final s in SyncOpStatus.values) s: 0};
    for (final op in _ops.values) {
      counts[op.status] = counts[op.status]! + 1;
    }
    return counts;
  }

  @override
  Future<void> resetInFlight() async {
    for (final op in _ops.values.toList()) {
      if (op.status == SyncOpStatus.inFlight) {
        _ops[op.id] = op.copyWith(status: SyncOpStatus.pending);
      }
    }
  }

  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) async {
    final key = _key(entity, localId);
    if (!_idMap.containsKey(key)) return null;
    return IdMapping(entity: entity, localId: localId, serverId: _idMap[key]);
  }

  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) async {
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
  }

  @override
  Future<void> putMapping(
          String entity, String localId, String? serverId) async =>
      _idMap[_key(entity, localId)] = serverId;

  @override
  Future<void> deleteMapping(String entity, String localId) async =>
      _idMap.remove(_key(entity, localId));

  @override
  Future<String?> getMeta(String key) async => _meta[key];

  @override
  Future<void> setMeta(String key, String? value) async {
    if (value == null) {
      _meta.remove(key);
    } else {
      _meta[key] = value;
    }
  }

  @override
  Future<void> clear() async {
    _ops.clear();
    _idMap.clear();
    _meta.clear();
  }
}
