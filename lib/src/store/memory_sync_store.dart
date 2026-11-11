import 'dart:async';
import 'dart:collection';
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
///
/// Like a database it is indexed: operations by `seq`, by id and by record,
/// and the id map in both directions, so a lookup costs the same with a
/// million rows as with ten.
class InMemorySyncStore extends SyncStore {
  /// Operations by `seq`, which is also the order they are pushed in.
  final SplayTreeMap<int, SyncOperation> _ops = SplayTreeMap();

  /// `seq` of each operation id.
  final Map<String, int> _seqOfId = {};

  /// The `seq`s of each record's operations, ascending.
  final Map<String, List<int>> _seqsOfRecord = {};

  final Map<SyncOpStatus, int> _counts = {
    for (final status in SyncOpStatus.values) status: 0,
  };

  /// Server id (null while local-only) by entity and local id.
  final Map<String, Map<String, String?>> _serverOf = {};

  /// Local id by entity and server id: a `String`, or a `List<String>` in
  /// the rare case several local records share one server id.
  final Map<String, Map<String, Object>> _localOf = {};

  final Map<String, String> _meta = {};
  int _seq = 0;

  /// Completes when the last queued call has finished; null when idle.
  Future<void>? _tail;

  /// How to undo each change of the open transaction, oldest first. Null
  /// outside transactions.
  List<void Function()>? _undo;

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
      // Only what the transaction changes is remembered, so its cost does
      // not depend on how much the store holds.
      final undo = _undo = [];
      _dirty = false;
      try {
        final result = await action();
        if (_dirty && _persistent) await _persist();
        return result;
      } catch (_) {
        _undo = null;
        for (final revert in undo.reversed) {
          revert();
        }
        rethrow;
      } finally {
        _undo = null;
        _dirty = false;
      }
    });
  }

  // State changes -------------------------------------------------------------
  //
  // Every change goes through one of these, which keep the indexes in step
  // and note how to undo it.

  /// Stores [op] at [seq]; null removes the operation there.
  void _setOp(int seq, SyncOperation? op) {
    final previous = _ops[seq];
    if (previous == null && op == null) return;
    _undo?.add(() => _setOp(seq, previous));
    if (previous != null) {
      _counts[previous.status] = _counts[previous.status]! - 1;
    }
    if (op == null) {
      _ops.remove(seq);
      _seqOfId.remove(previous!.id);
      final key = _key(previous.entity, previous.localId);
      final seqs = _seqsOfRecord[key]!..remove(seq);
      if (seqs.isEmpty) _seqsOfRecord.remove(key);
      return;
    }
    _ops[seq] = op;
    _counts[op.status] = _counts[op.status]! + 1;
    if (previous == null) {
      _seqOfId[op.id] = seq;
      final seqs = _seqsOfRecord.putIfAbsent(_key(op.entity, op.localId), () {
        return <int>[];
      });
      // Appended in order, except when a rollback puts an operation back.
      var at = seqs.length;
      while (at > 0 && seqs[at - 1] > seq) {
        at--;
      }
      seqs.insert(at, seq);
    }
  }

  void _setSeq(int value) {
    final previous = _seq;
    _undo?.add(() => _seq = previous);
    _seq = value;
  }

  /// Sets the mapping of ([entity], [localId]), or removes it when
  /// [present] is false.
  void _setMapping(String entity, String localId,
      {required bool present, String? serverId}) {
    final forward = _serverOf[entity];
    final had = forward != null && forward.containsKey(localId);
    final previous = had ? forward[localId] : null;
    if (had == present && previous == serverId) return;
    _undo?.add(
        () => _setMapping(entity, localId, present: had, serverId: previous));

    if (previous != null) {
      final reverse = _localOf[entity]!;
      final current = reverse[previous];
      if (current is List<String>) {
        current.remove(localId);
        if (current.length == 1) reverse[previous] = current.single;
      } else if (current == localId) {
        reverse.remove(previous);
      }
    }
    if (!present) {
      forward!.remove(localId);
      return;
    }
    (_serverOf[entity] ??= {})[localId] = serverId;
    if (serverId != null) {
      final reverse = _localOf[entity] ??= {};
      final current = reverse[serverId];
      if (current == null) {
        reverse[serverId] = localId;
      } else if (current is List<String>) {
        current.add(localId);
      } else {
        reverse[serverId] = <String>[current as String, localId];
      }
    }
  }

  void _setMetaValue(String key, String? value) {
    final previous = _meta[key];
    if (previous == value) return;
    _undo?.add(() => _setMetaValue(key, previous));
    if (value == null) {
      _meta.remove(key);
    } else {
      _meta[key] = value;
    }
  }

  String? _localIdOf(String entity, String serverId) {
    final found = _localOf[entity]?[serverId];
    return found is List<String> ? found.first : found as String?;
  }

  // Outbox --------------------------------------------------------------------

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
        _setSeq(_seq + 1);
        _setOp(stored.seq!, stored);
        return _copy(stored);
      });

  @override
  Future<void> updateOperation(SyncOperation operation) => _write(() {
        final seq = _seqOfId[operation.id];
        if (seq == null) return;
        // An operation stays the change of the record it was recorded for.
        final existing = _ops[seq]!;
        final stored = _stored(operation, seq);
        if (stored.entity == existing.entity &&
            stored.localId == existing.localId) {
          _setOp(seq, stored);
        } else {
          _setOp(seq, null);
          _setOp(seq, stored);
        }
      });

  @override
  Future<void> deleteOperation(String id) => _write(() {
        final seq = _seqOfId[id];
        if (seq != null) _setOp(seq, null);
      });

  @override
  Future<SyncOperation?> operationById(String id) => _serialized(() {
        final seq = _seqOfId[id];
        return seq == null ? null : _copy(_ops[seq]!);
      });

  @override
  Future<List<SyncOperation>> operations({Set<SyncOpStatus>? statuses}) =>
      _serialized(() => [
            for (final op in _ops.values)
              if (statuses == null || statuses.contains(op.status)) _copy(op),
          ]);

  @override
  Future<List<SyncOperation>> operationsFor(String entity, String localId) =>
      _serialized(() => [
            for (final seq
                in _seqsOfRecord[_key(entity, localId)] ?? const <int>[])
              _copy(_ops[seq]!),
          ]);

  @override
  Future<Map<SyncOpStatus, int>> countByStatus() =>
      _serialized(() => Map.of(_counts));

  @override
  Future<void> resetInFlight() => _write(() {
        if (_counts[SyncOpStatus.inFlight] == 0) return;
        for (final op in _ops.values.toList()) {
          if (op.status == SyncOpStatus.inFlight) {
            _setOp(op.seq!, op.copyWith(status: SyncOpStatus.pending));
          }
        }
      });

  // Id map --------------------------------------------------------------------

  @override
  Future<IdMapping?> mappingForLocal(String entity, String localId) =>
      _serialized(() => _mapping(entity, localId));

  IdMapping? _mapping(String entity, String localId) {
    final forward = _serverOf[entity];
    if (forward == null || !forward.containsKey(localId)) return null;
    return IdMapping(
        entity: entity, localId: localId, serverId: forward[localId]);
  }

  @override
  Future<IdMapping?> mappingForServer(String entity, String serverId) =>
      _serialized(() {
        final localId = _localIdOf(entity, serverId);
        return localId == null
            ? null
            : IdMapping(entity: entity, localId: localId, serverId: serverId);
      });

  @override
  Future<void> putMapping(String entity, String localId, String? serverId) =>
      _write(() =>
          _setMapping(entity, localId, present: true, serverId: serverId));

  @override
  Future<void> deleteMapping(String entity, String localId) =>
      _write(() => _setMapping(entity, localId, present: false));

  // Key/value -----------------------------------------------------------------

  @override
  Future<String?> getMeta(String key) => _serialized(() => _meta[key]);

  @override
  Future<void> setMeta(String key, String? value) =>
      _write(() => _setMetaValue(key, value));

  @override
  Future<void> clear() => _write(() {
        final undo = _undo;
        if (undo != null) {
          // Rare, so a copy of the state is good enough here.
          final snapshot = _snapshot();
          undo.add(() => _restore(snapshot));
        }
        _reset();
      });

  void _reset() {
    _ops.clear();
    _seqOfId.clear();
    _seqsOfRecord.clear();
    _counts.updateAll((_, __) => 0);
    _serverOf.clear();
    _localOf.clear();
    _meta.clear();
  }

  // Paged and batched access --------------------------------------------------

  @override
  Future<List<SyncOperation>> operationsPage({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
    DateTime? dueAt,
    int afterSeq = 0,
    required int limit,
  }) =>
      _serialized(() {
        final page = <SyncOperation>[];
        if (limit <= 0) return page;
        for (var seq = _ops.firstKeyAfter(afterSeq);
            seq != null;
            seq = _ops.firstKeyAfter(seq)) {
          final op = _ops[seq]!;
          if (statuses != null && !statuses.contains(op.status)) continue;
          if (entities != null && !entities.contains(op.entity)) continue;
          final next = op.nextAttemptAt;
          if (dueAt != null && next != null && next.isAfter(dueAt)) continue;
          page.add(_copy(op));
          if (page.length == limit) break;
        }
        return page;
      });

  @override
  Future<List<SyncOperation>> operationsForRecords(
          String entity, Iterable<String> localIds) =>
      _serialized(() {
        final seqs = <int>[
          for (final localId in localIds.toSet())
            ...?_seqsOfRecord[_key(entity, localId)],
        ]..sort();
        return [for (final seq in seqs) _copy(_ops[seq]!)];
      });

  @override
  Future<void> markOperations(Iterable<String> ids, SyncOpStatus status) =>
      _write(() {
        for (final id in ids) {
          final seq = _seqOfId[id];
          if (seq == null) continue;
          final op = _ops[seq]!;
          if (op.status != status) _setOp(seq, op.copyWith(status: status));
        }
      });

  @override
  Future<void> deleteOperations(Iterable<String> ids) => _write(() {
        for (final id in ids) {
          final seq = _seqOfId[id];
          if (seq != null) _setOp(seq, null);
        }
      });

  @override
  Future<int> countOperations({
    Set<SyncOpStatus>? statuses,
    Set<String>? entities,
  }) =>
      _serialized(() {
        if (entities == null) {
          var total = 0;
          for (final status in statuses ?? SyncOpStatus.values) {
            total += _counts[status]!;
          }
          return total;
        }
        var total = 0;
        for (final op in _ops.values) {
          if (entities.contains(op.entity) &&
              (statuses == null || statuses.contains(op.status))) {
            total++;
          }
        }
        return total;
      });

  @override
  Future<DateTime?> nextAttemptAfter(DateTime now) => _serialized(() {
        DateTime? earliest;
        for (final op in _ops.values) {
          final next = op.nextAttemptAt;
          if (next != null &&
              op.status == SyncOpStatus.pending &&
              next.isAfter(now) &&
              (earliest == null || next.isBefore(earliest))) {
            earliest = next;
          }
        }
        return earliest;
      });

  @override
  Future<void> clearBackoff(DateTime now, {Set<String>? entities}) =>
      _write(() {
        for (final op in _ops.values.toList()) {
          final next = op.nextAttemptAt;
          if (next == null ||
              op.status != SyncOpStatus.pending ||
              !next.isAfter(now)) {
            continue;
          }
          if (entities != null && !entities.contains(op.entity)) continue;
          _setOp(op.seq!, op.copyWith(clearNextAttemptAt: true));
        }
      });

  @override
  Future<Set<String>> localIdsWithOperations(
          String entity, Iterable<String> localIds) =>
      _serialized(() => {
            for (final localId in localIds)
              if (_seqsOfRecord.containsKey(_key(entity, localId))) localId,
          });

  @override
  Future<Map<String, String>> localIdsForServer(
          String entity, Iterable<String> serverIds) =>
      _serialized(() {
        final found = <String, String>{};
        for (final serverId in serverIds) {
          final localId = _localIdOf(entity, serverId);
          if (localId != null) found[serverId] = localId;
        }
        return found;
      });

  @override
  Future<Map<String, IdMapping>> mappingsForLocal(
          String entity, Iterable<String> localIds) =>
      _serialized(() {
        final found = <String, IdMapping>{};
        for (final localId in localIds) {
          final mapping = _mapping(entity, localId);
          if (mapping != null) found[localId] = mapping;
        }
        return found;
      });

  @override
  Future<void> putMappings(String entity, Map<String, String?> localToServer) {
    if (localToServer.isEmpty) return Future.value();
    return _write(() {
      localToServer.forEach((localId, serverId) =>
          _setMapping(entity, localId, present: true, serverId: serverId));
    });
  }

  // Snapshots (KeyValueSyncStore) -------------------------------------------

  static const _snapshotVersion = 1;

  Map<String, Object?> _snapshot() => {
        'version': _snapshotVersion,
        'seq': _seq,
        'ops': [
          for (final op in _ops.values) {...op.toRow(), 'seq': op.seq},
        ],
        'ids': [
          for (final entity in _serverOf.entries)
            for (final entry in entity.value.entries)
              [entity.key, entry.key, entry.value],
        ],
        'meta': Map.of(_meta),
      };

  void _restore(Map<String, Object?> snapshot) {
    final version = snapshot['version'];
    if (version != _snapshotVersion) {
      throw FormatException('Unsupported sync state version: $version');
    }
    // Read everything before changing anything: a snapshot that cannot be
    // read must leave the store as it was.
    final ops = <SyncOperation>[
      for (final row in snapshot['ops']! as List)
        SyncOperation.fromRow(Map<String, Object?>.from(row as Map)),
    ];
    final ids = <(String, String, String?)>[
      for (final entry in snapshot['ids']! as List)
        (
          (entry as List)[0] as String,
          entry[1] as String,
          entry[2] as String?,
        ),
    ];
    final meta = Map<String, String>.from(snapshot['meta']! as Map);
    final seq = (snapshot['seq']! as num).toInt();

    final undo = _undo;
    _undo = null;
    try {
      _reset();
      for (final op in ops) {
        _setOp(op.seq!, op);
      }
      for (final (entity, localId, serverId) in ids) {
        _setMapping(entity, localId, present: true, serverId: serverId);
      }
      _meta.addAll(meta);
      _seq = seq;
    } finally {
      _undo = undo;
    }
  }
}
