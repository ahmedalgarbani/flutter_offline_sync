import 'dart:async';
import 'dart:developer' as developer;

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';

import '../connectivity.dart';
import '../core/conflict.dart';
import '../core/entity_config.dart';
import '../core/ids.dart';
import '../core/operation.dart';
import '../core/outcomes.dart';
import '../core/references.dart';
import '../core/status.dart';
import '../store/sync_store.dart';
import 'sync_config.dart';

/// Offline-first sync engine.
///
/// The app always reads and writes its local database. After each write it
/// records the change with [recordCreate], [recordUpdate] or [recordDelete]
/// (ideally in the same transaction). The engine then:
///
/// 1. pushes the outbox in order, holding a record back until every record
///    it references has a server id, and rewriting local ids to server ids;
/// 2. pulls server changes entity by entity, parents first, converting
///    server ids back to local ids and never overwriting unpushed changes
///    without asking the entity's conflict resolver.
///
/// Only one run (push and/or pull) happens at a time. Work requested while
/// a run is in progress is merged into a single follow-up run.
class SyncEngine with WidgetsBindingObserver {
  SyncEngine({
    required this.store,
    required List<SyncEntityConfig> entities,
    ConnectivitySource? connectivity,
    this.config = const SyncConfig(),
  })  : connectivity = connectivity ?? const AlwaysOnline(),
        _entities = _index(entities) {
    _pullOrder = _dependencyOrder(entities);
  }

  final SyncStore store;
  final ConnectivitySource connectivity;
  final SyncConfig config;

  final Map<String, SyncEntityConfig> _entities;
  late final List<String> _pullOrder;

  final ValueNotifier<SyncStatus> _status = ValueNotifier(const SyncStatus());
  final StreamController<SyncEvent> _events = StreamController.broadcast();

  Future<void>? _initFuture;
  bool _initialized = false;
  bool _started = false;
  bool _disposed = false;
  bool _observingLifecycle = false;
  StreamSubscription<bool>? _connectivitySub;
  Timer? _periodic;
  Timer? _debounceTimer;
  Timer? _retryTimer;
  bool _debouncedPull = false;

  /// The run in progress, and the request it serves.
  Future<SyncRunResult>? _running;
  _RunRequest? _active;

  /// Work requested during a run; it starts as soon as the run ends.
  _RunRequest? _queued;

  /// While positive no run may start (see [_exclusive]).
  int _holds = 0;

  /// Zone value marking code that runs as part of a run: the run's request.
  static final Object _runZoneKey = Object();

  /// Current state, for `ValueListenableBuilder` / `SyncStatusBuilder`.
  ValueListenable<SyncStatus> get status => _status;

  /// Fine-grained notifications.
  Stream<SyncEvent> get events => _events.stream;

  /// Entity names in the order they are pulled (parents first).
  List<String> get pullOrder => List.unmodifiable(_pullOrder);

  /// A new random local id (UUID v4). Using UUIDs as local ids is the
  /// safest choice, but integer ids work too.
  static String newLocalId() => SyncIds.uuid();

  /// True in code called by the run in progress (adapters, resolvers,
  /// callbacks), which must not wait for that run to end.
  bool get _inRun {
    final marker = Zone.current[_runZoneKey];
    return marker != null && identical(marker, _active);
  }

  // Lifecycle -----------------------------------------------------------------

  /// Prepares the store and installs the automatic triggers.
  ///
  /// If the store fails to open, the error is rethrown and [start] can be
  /// called again.
  Future<void> start() async {
    if (_started || _disposed) return;
    _started = true;
    try {
      await _ensureInit();
    } catch (_) {
      _started = false;
      rethrow;
    }
    if (_disposed) return;

    _setStatus(_status.value.copyWith(isOnline: connectivity.isOnline));
    _connectivitySub = connectivity.onChanged.listen(
      (online) {
        _setStatus(_status.value.copyWith(isOnline: online));
        if (online && config.syncOnReconnect) _schedule(pull: true);
      },
      onError: (Object error, StackTrace stackTrace) => _log(
          SyncLogLevel.warning,
          'Connectivity stream failed',
          error,
          stackTrace),
    );

    final interval = config.periodicInterval;
    if (interval != null) {
      _periodic = Timer.periodic(interval, (_) => _schedule(pull: true));
    }

    if (config.syncOnResume) {
      try {
        WidgetsBinding.instance.addObserver(this);
        _observingLifecycle = true;
      } catch (_) {
        // No Flutter binding (pure Dart use): skip resume triggers.
      }
    }

    if (config.syncOnStart) {
      _schedule(pull: true, immediate: true);
    } else {
      // Operations may still be waiting out a backoff from a previous
      // session.
      final next = await _scheduleRetry();
      _setStatus(_status.value
          .copyWith(nextRetryAt: next, clearNextRetryAt: next == null));
    }
  }

  /// Stops triggers. A run in progress finishes normally; work queued
  /// behind it is dropped.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _debounceTimer?.cancel();
    _periodic?.cancel();
    _retryTimer?.cancel();
    if (_observingLifecycle) {
      WidgetsBinding.instance.removeObserver(this);
      _observingLifecycle = false;
    }
    final queued = _queued;
    _queued = null;
    queued?.completer
        .complete(SyncRunResult()..abortedBy = SyncAbortReason.paused);
    await _connectivitySub?.cancel();
    final running = _running;
    if (running != null && !_inRun) await running;
    await _events.close();
    _status.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) _schedule(pull: true);
  }

  Future<void> _ensureInit() {
    // Once ready, answer with a new future rather than the stored one, which
    // may belong to another zone (for example a test's setUp).
    if (_initialized) return Future<void>.value();
    return _initFuture ??= _init();
  }

  Future<void> _init() async {
    try {
      await store.init();
      await store.resetInFlight();
    } catch (_) {
      // Let the next call try again instead of failing forever.
      _initFuture = null;
      rethrow;
    }
    _initialized = true;
    await _refreshCounts();
  }

  // Recording local writes ----------------------------------------------------

  /// Records that [localId] of [entity] was created locally with [data].
  ///
  /// Call it right after inserting the row, inside the same database
  /// transaction when the store shares your database.
  Future<SyncOperation?> recordCreate(
    String entity,
    Object localId,
    Map<String, dynamic> data,
  ) =>
      _record(SyncOpType.create, entity, localId, data);

  /// Records a local update. [data] may be the full record or only the
  /// changed fields (it is merged into a not-yet-pushed operation).
  Future<SyncOperation?> recordUpdate(
    String entity,
    Object localId,
    Map<String, dynamic> data,
  ) =>
      _record(SyncOpType.update, entity, localId, data);

  /// Records a local delete. If the record never reached the server, its
  /// pending operations are simply dropped and nothing is sent.
  Future<SyncOperation?> recordDelete(
    String entity,
    Object localId, {
    Map<String, dynamic> data = const {},
  }) =>
      _record(SyncOpType.delete, entity, localId, data);

  /// Declares that [localId] of [entity] is created on the server as a side
  /// effect of pushing [viaEntity]/[viaLocalId], for endpoints that create
  /// several records at once (e.g. account + profile + customer).
  ///
  /// Records that reference it wait until that push is done. Register the
  /// resulting server id with [registerMapping], typically from the via
  /// entity's `LocalAdapter.onServerIdAssigned`.
  Future<void> recordCreatedVia(
    String entity,
    Object localId, {
    required String viaEntity,
    required Object viaLocalId,
  }) async {
    final id = SyncIds.normalize(localId);
    final via = SyncIds.normalize(viaLocalId);
    if (id == null || via == null) {
      throw ArgumentError('Both localId and viaLocalId are required');
    }
    _config(entity);
    _config(viaEntity);
    await _ensureInit();
    await store.transaction(() async {
      if (await store.mappingForLocal(entity, id) == null) {
        await store.putMapping(entity, id, null);
      }
      await store.setMeta(_viaKey(entity, id), '$viaEntity\u0000$via');
    });
  }

  Future<SyncOperation?> _record(
    SyncOpType type,
    String entity,
    Object localId,
    Map<String, dynamic> data,
  ) async {
    final cfg = _config(entity);
    if (!cfg.pushEnabled) {
      throw StateError('Entity "$entity" is read-only (pushEnabled: false).');
    }
    final id = SyncIds.normalize(localId);
    if (id == null) {
      throw ArgumentError.value(localId, 'localId', 'must be a non-empty id');
    }
    await _ensureInit();
    final now = _now();
    // The outbox keeps its own copy: later changes to [data] by the caller
    // must not alter what is sent.
    final change = deepCopyJson(data);

    final op = await store.transaction(() async {
      final existing = await store.operationsFor(entity, id);
      final mapping = await store.mappingForLocal(entity, id);

      if (type == SyncOpType.create && mapping == null) {
        // Registers the id as local-only: references to it are never sent
        // to the server as if they were server ids.
        await store.putMapping(entity, id, null);
      }

      if (type == SyncOpType.delete) {
        final neverReachedServer = (mapping == null || mapping.isLocalOnly) &&
            existing.any((o) => o.type == SyncOpType.create) &&
            existing.every((o) => o.status != SyncOpStatus.inFlight);
        if (neverReachedServer) {
          for (final o in existing) {
            await store.deleteOperation(o.id);
          }
          return null;
        }
        if (cfg.coalesce) {
          for (final o in existing) {
            if (o.type == SyncOpType.update &&
                o.status != SyncOpStatus.inFlight) {
              await store.deleteOperation(o.id);
            }
          }
        }
      } else if (cfg.coalesce) {
        // Everything queued after the operation being sent (if any) can
        // still be merged into one operation.
        final inFlight =
            existing.lastIndexWhere((o) => o.status == SyncOpStatus.inFlight);
        final open = existing.sublist(inFlight + 1);
        if (open.isNotEmpty) {
          if (open.any((o) => o.type == SyncOpType.delete)) {
            throw StateError('$entity/$id is already deleted.');
          }
          final merged = _fold(cfg, open, change, now);
          for (final o in open.skip(1)) {
            await store.deleteOperation(o.id);
          }
          await store.updateOperation(merged);
          return merged;
        }
      }

      return store.insertOperation(SyncOperation(
        id: SyncIds.uuid(),
        entity: entity,
        localId: id,
        type: type,
        payload: change,
        createdAt: now,
        updatedAt: now,
      ));
    });

    _emit(SyncEvent(SyncEventType.operationRecorded,
        entity: entity, localId: id, message: type.name));
    await _refreshCounts();
    if (config.autoPushAfterWrite) _schedule(pull: false);
    return op;
  }

  /// Folds the unsent operations of one record and a new [change] into the
  /// first of them. Editing a failed record (e.g. fixing a validation
  /// error) queues it again with a fresh attempt budget.
  SyncOperation _fold(
    SyncEntityConfig cfg,
    List<SyncOperation> open,
    Map<String, dynamic> change,
    DateTime now,
  ) {
    final first = open.first;
    var payload = first.payload;
    for (final o in open.skip(1)) {
      payload = cfg.mergePayload(payload, o.payload);
    }
    payload = cfg.mergePayload(payload, change);
    final anyFailed = open.any((o) => o.isFailed);
    return first.copyWith(
      payload: payload,
      updatedAt: now,
      status: SyncOpStatus.pending,
      force: open.any((o) => o.force),
      attempts: anyFailed ? 0 : first.attempts,
      clearNextAttemptAt: anyFailed,
      clearLastError: anyFailed,
    );
  }

  // Running -------------------------------------------------------------------

  /// Pushes the outbox, then pulls (unless [pull] is false), optionally only
  /// for [entities].
  ///
  /// If a run is in progress, the request is merged into one follow-up run
  /// that starts when the current one ends, and the returned future
  /// completes with that follow-up's result. A write made during a run is
  /// therefore always pushed, and a pull is never lost.
  Future<SyncRunResult> syncNow({bool? pull, Set<String>? entities}) =>
      _request(push: true, pull: pull ?? config.pullOnSync, only: entities);

  /// Pushes without pulling.
  Future<SyncRunResult> pushNow({Set<String>? entities}) =>
      _request(push: true, pull: false, only: entities);

  /// Pulls without pushing first. Prefer [syncNow], which pushes first so
  /// the server already has local changes.
  Future<SyncRunResult> pullNow({Set<String>? entities}) =>
      _request(push: false, pull: true, only: entities);

  Future<SyncRunResult> _request({
    required bool push,
    required bool pull,
    Set<String>? only,
  }) {
    if (_disposed) {
      return Future.value(SyncRunResult()..abortedBy = SyncAbortReason.paused);
    }
    if (_running == null && _holds == 0) {
      return _startRun(_RunRequest(push: push, pull: pull, only: only));
    }
    final queued = _queued;
    if (queued != null) {
      queued.merge(push: push, pull: pull, only: only);
      return queued.completer.future;
    }
    final request = _RunRequest(push: push, pull: pull, only: only);
    _queued = request;
    return request.completer.future;
  }

  Future<SyncRunResult> _startRun(_RunRequest request) {
    _active = request;
    final future =
        runZoned(() => _run(request), zoneValues: {_runZoneKey: request});
    _running = future;
    // _run never throws.
    future.then((_) {
      _running = null;
      _active = null;
      _startQueued();
    });
    return future;
  }

  void _startQueued() {
    final next = _queued;
    if (next == null || _running != null || _holds > 0 || _disposed) return;
    _queued = null;
    next.completer.complete(_startRun(next));
  }

  /// Runs [action] while no run is in progress: waits for the current run
  /// and holds new ones back until [action] completes.
  Future<T> _exclusive<T>(Future<T> Function() action) async {
    if (_inRun) return action();
    _holds++;
    try {
      while (_running != null) {
        await _running;
      }
      return await action();
    } finally {
      _holds--;
      _startQueued();
    }
  }

  void _schedule({required bool pull, bool immediate = false}) {
    if (_disposed || !_started) return;
    _debouncedPull = _debouncedPull || pull;
    _debounceTimer?.cancel();
    _debounceTimer = Timer(immediate ? Duration.zero : config.debounce, () {
      final shouldPull = _debouncedPull && config.pullOnSync;
      _debouncedPull = false;
      if (!connectivity.isOnline) {
        _setStatus(
            _status.value.copyWith(phase: SyncPhase.offline, isOnline: false));
        return;
      }
      syncNow(pull: shouldPull);
    });
  }

  Future<SyncRunResult> _run(_RunRequest request) async {
    final result = SyncRunResult();
    final stopwatch = Stopwatch()..start();
    var crashed = false;
    _emit(const SyncEvent(SyncEventType.runStarted));
    try {
      await _ensureInit();
      final gate = config.canSync;
      if (gate != null && !await gate()) {
        result.abortedBy = SyncAbortReason.paused;
      } else if (!connectivity.isOnline) {
        result.abortedBy = SyncAbortReason.offline;
      } else {
        if (request.push) await _push(result, request.only);
        if (result.abortedBy == null && request.pull) {
          await _pull(result, request.only);
        }
      }
    } catch (e, st) {
      crashed = true;
      result.errors.add(e.toString());
      _log(SyncLogLevel.error, 'Sync run failed', e, st);
    }
    stopwatch.stop();
    result.duration = stopwatch.elapsed;

    final nextRetry = await _scheduleRetry();
    if (!_disposed) {
      final phase = switch (result.abortedBy) {
        SyncAbortReason.offline => SyncPhase.offline,
        SyncAbortReason.unauthorized => SyncPhase.authRequired,
        SyncAbortReason.paused => SyncPhase.paused,
        null => SyncPhase.idle,
      };
      _setStatus(_status.value.copyWith(
        phase: phase,
        isOnline: result.abortedBy == SyncAbortReason.offline
            ? false
            : connectivity.isOnline,
        clearCurrentEntity: true,
        lastSyncedAt: result.completed && !crashed ? _now() : null,
        lastError: result.errors.isEmpty ? null : result.errors.last,
        clearLastError: result.errors.isEmpty,
        nextRetryAt: nextRetry,
        clearNextRetryAt: nextRetry == null,
      ));
      await _refreshCounts();
    }
    _emit(SyncEvent(SyncEventType.runFinished, message: result.toString()));
    _log(SyncLogLevel.info, 'Sync run finished: $result');
    return result;
  }

  /// Arms a timer for the earliest operation waiting out a backoff delay,
  /// so it is retried without waiting for another trigger. Returns its time.
  Future<DateTime?> _scheduleRetry() async {
    _retryTimer?.cancel();
    _retryTimer = null;
    if (_disposed) return null;
    final List<SyncOperation> pending;
    try {
      pending = await store.operations(statuses: {SyncOpStatus.pending});
    } catch (e, st) {
      _log(SyncLogLevel.warning, 'Could not read the outbox', e, st);
      return null;
    }
    final now = _now();
    DateTime? earliest;
    for (final op in pending) {
      final next = op.nextAttemptAt;
      // Operations whose time has passed but were not pushed are blocked by
      // something else (a parent, a failed operation of the same record).
      if (next != null &&
          next.isAfter(now) &&
          (earliest == null || next.isBefore(earliest))) {
        earliest = next;
      }
    }
    if (earliest == null || _disposed) return null;
    if (_started) {
      _retryTimer = Timer(earliest.difference(now),
          () => _schedule(pull: false, immediate: true));
    }
    return earliest;
  }

  // Push ----------------------------------------------------------------------

  Future<void> _push(SyncRunResult result, Set<String>? only) async {
    for (var pass = 0; pass < config.maxPushPasses; pass++) {
      final progress = await _pushPass(result, only);
      if (result.abortedBy != null || !progress) break;
    }
    final left = await store.operations(statuses: {SyncOpStatus.pending});
    result.waiting =
        left.where((o) => only == null || only.contains(o.entity)).length;
  }

  /// One ordered walk over the outbox. Returns true if anything changed
  /// that could unblock other operations.
  Future<bool> _pushPass(SyncRunResult result, Set<String>? only) async {
    final ops = await store
        .operations(statuses: {SyncOpStatus.pending, SyncOpStatus.failed});
    final blocked = <String>{
      for (final op in ops)
        if (op.isFailed) _recordKey(op.entity, op.localId),
    };
    var progress = false;

    for (final listed in ops) {
      if (!listed.isPending) continue;
      if (only != null && !only.contains(listed.entity)) continue;
      final key = _recordKey(listed.entity, listed.localId);
      if (blocked.contains(key)) continue;
      final next = listed.nextAttemptAt;
      if (next != null && next.isAfter(_now())) {
        blocked.add(key);
        continue;
      }
      final cfg = _entities[listed.entity];
      if (cfg == null) {
        await _markFailed(listed, 'Unknown entity "${listed.entity}"', result);
        blocked.add(key);
        continue;
      }

      final op = await _claim(listed.id);
      if (op == null) continue;

      _setCurrent(SyncPhase.pushing, op.entity);
      switch (await _pushOne(op, cfg, result)) {
        case _Step.pushed:
          progress = true;
        case _Step.held:
          blocked.add(key);
        case _Step.resolved:
          blocked.add(key);
          progress = true;
      }
      await _refreshCounts();
      if (result.abortedBy != null) break;
    }
    return progress;
  }

  /// Re-reads and claims an operation: the app may have merged new data
  /// into it since the list was loaded.
  Future<SyncOperation?> _claim(String id) => store.transaction(() async {
        final fresh = await store.operationById(id);
        if (fresh == null || !fresh.isPending) return null;
        final claimed = fresh.copyWith(status: SyncOpStatus.inFlight);
        await store.updateOperation(claimed);
        return claimed;
      });

  /// Sends one claimed operation. Never leaves it in flight unless the store
  /// itself fails.
  Future<_Step> _pushOne(
    SyncOperation op,
    SyncEntityConfig cfg,
    SyncRunResult result,
  ) async {
    final PushRequest request;
    try {
      switch (await _prepare(op, cfg)) {
        case _Ready(request: final ready):
          request = ready;
        case _Wait(:final reason):
          await _release(op, reason);
          return _Step.held;
        case _Reject(:final reason):
          await _markFailed(op, reason, result);
          return _Step.held;
        case _Resolved():
          throw StateError('unreachable');
      }
    } catch (e, st) {
      // A local failure, e.g. buildPushPayload threw: retry with backoff.
      _log(SyncLogLevel.warning, 'Preparing ${op.entity}/${op.localId} failed',
          e, st);
      await _backoff(op, e, result);
      return _Step.held;
    }

    PushOutcome outcome;
    try {
      outcome = await cfg.remote.push(request);
    } catch (e, st) {
      outcome = _classify(e);
      _log(
          SyncLogLevel.warning, 'Push ${op.entity}/${op.localId} threw', e, st);
    }

    switch (outcome) {
      case PushSuccess():
        return await _onPushSuccess(op, cfg, request, outcome, result)
            ? _Step.pushed
            : _Step.held;
      case PushNetworkError(:final error):
        await _release(op, error.toString());
        result.abortedBy = SyncAbortReason.offline;
        return _Step.held;
      case PushUnauthorized(:final error):
        await _release(op, (error ?? 'Unauthorized').toString());
        result.abortedBy = SyncAbortReason.unauthorized;
        return _Step.held;
      case PushRetry(:final error, :final retryAfter):
        await _backoff(op, error, result, retryAfter: retryAfter);
        return _Step.held;
      case PushRejected(:final error):
        await _markFailed(op, error, result);
        return _Step.held;
      case PushConflict():
        try {
          await _onPushConflict(op, cfg, request, outcome, result);
        } catch (e, st) {
          _log(
              SyncLogLevel.warning,
              'Resolving the conflict on ${op.entity}/${op.localId} failed',
              e,
              st);
          await _backoff(op, e, result);
          return _Step.held;
        }
        return _Step.resolved;
    }
  }

  /// Puts a claimed operation back in the queue without counting an
  /// attempt.
  Future<void> _release(SyncOperation op, String reason) =>
      store.updateOperation(
          op.copyWith(status: SyncOpStatus.pending, lastError: reason));

  PushOutcome _classify(Object error) {
    try {
      return config.classifyError(error);
    } catch (e, st) {
      _log(SyncLogLevel.error, 'classifyError threw', e, st);
      return PushOutcome.retry(error);
    }
  }

  Future<_Prepared> _prepare(SyncOperation op, SyncEntityConfig cfg) async {
    var type = op.type;
    final serverId = await _serverIdFor(cfg, op.localId);
    if (type != SyncOpType.create && serverId == null) {
      final waitReason = await _pendingCreatorOf(op.entity, op.localId);
      if (waitReason != null) return _Wait(waitReason);
      return _Reject('${op.entity}/${op.localId} has no server id: '
          'its create never reached the server');
    }
    if (type == SyncOpType.create && serverId != null) {
      // Already created (e.g. adopted from the server): send as update.
      type = SyncOpType.update;
    }

    var payload = await cfg.local.buildPushPayload(op) ?? op.payload;
    for (final reference in cfg.references) {
      final resolved = <String, String?>{};
      for (final value in reference.read(payload)) {
        final localRef = SyncIds.normalize(value);
        if (localRef == null || resolved.containsKey(localRef)) continue;
        final resolution = await _resolveOutbound(
            reference.target, localRef, cfg.unknownReferencePolicy);
        switch (resolution) {
          case _Resolved(:final serverId):
            resolved[localRef] = serverId;
          default:
            return resolution;
        }
      }
      if (resolved.values.any((v) => v != null)) {
        final toJson = _entities[reference.target]!.serverIdToJson;
        payload = reference.rewrite(payload, (value) {
          final serverRef = resolved[SyncIds.normalize(value)];
          if (serverRef == null) return value;
          return toJson != null
              ? toJson(serverRef)
              : SyncIds.toJsonValue(serverRef, value);
        });
      }
    }

    final sendOp = type == op.type ? op : op.copyWith(type: type);
    return _Ready(PushRequest(
      operation: sendOp,
      payload: payload,
      serverId: serverId,
    ));
  }

  Future<_Prepared> _resolveOutbound(
    String target,
    String localRef,
    UnknownReferencePolicy policy,
  ) async {
    final mapping = await store.mappingForLocal(target, localRef);
    if (mapping?.serverId != null) return _Resolved(mapping!.serverId);

    final targetOps = await store.operationsFor(target, localRef);
    if (targetOps.any((o) => o.isFailed)) {
      return _Wait('Waiting for $target/$localRef, whose sync failed');
    }
    if (targetOps.any((o) => o.type == SyncOpType.create)) {
      return _Wait('Waiting for $target/$localRef to be pushed');
    }
    if (mapping != null) {
      final waitReason = await _pendingCreatorOf(target, localRef);
      if (waitReason != null) return _Wait(waitReason);
      return _Reject('Referenced $target/$localRef was created on this '
          'device but was deleted or discarded before reaching the server');
    }

    final targetCfg = _entities[target];
    final legacy =
        SyncIds.normalize(await targetCfg?.local.findServerId(localRef));
    if (legacy != null) {
      await store.putMapping(target, localRef, legacy);
      return _Resolved(legacy);
    }
    if (policy == UnknownReferencePolicy.reject) {
      return _Reject('Unknown reference $target/$localRef');
    }
    return const _Resolved(null);
  }

  /// Wait reason when [entity]/[localId] is created by another record's
  /// push that is still in the outbox (see [recordCreatedVia]).
  Future<String?> _pendingCreatorOf(String entity, String localId) async {
    final key = _viaKey(entity, localId);
    final via = await store.getMeta(key);
    if (via == null) return null;
    final parts = via.split('\u0000');
    final viaOps = await store.operationsFor(parts[0], parts[1]);
    if (viaOps.isEmpty) {
      // The creating push is done (or discarded): forget the link.
      await store.setMeta(key, null);
      return null;
    }
    return 'Waiting for ${parts[0]}/${parts[1]}, which creates '
        '$entity/$localId';
  }

  Future<bool> _onPushSuccess(
    SyncOperation op,
    SyncEntityConfig cfg,
    PushRequest request,
    PushSuccess outcome,
    SyncRunResult result,
  ) async {
    String? serverId = request.serverId;
    final isCreate = request.type == SyncOpType.create;
    if (isCreate) {
      serverId = SyncIds.normalize(outcome.serverId) ??
          (outcome.record == null
              ? null
              : SyncIds.normalize(cfg.serverIdOf(outcome.record!)));
      if (serverId == null) {
        // Retrying would create a duplicate, so stop and surface it.
        await _markFailed(
            op,
            'The server accepted the create but returned no id. '
            'Return PushOutcome.success(serverId: ...) from the adapter.',
            result);
        return false;
      }
    }

    try {
      await store.transaction(() async {
        if (isCreate) {
          await store.putMapping(op.entity, op.localId, serverId);
          await cfg.local.onServerIdAssigned(op.localId, serverId!,
              serverRecord: outcome.record);
        }
        await store.deleteOperation(op.id);
        await cfg.local.onPushed(op, serverId: serverId);
      });
    } catch (e, st) {
      // The server has the change: never send it again, even though the
      // app's own bookkeeping failed.
      _log(SyncLogLevel.error,
          'Recording the push of ${op.entity}/${op.localId} failed', e, st);
      result.errors.add('${op.entity}/${op.localId}: $e');
      await store.transaction(() async {
        if (isCreate) await store.putMapping(op.entity, op.localId, serverId);
        await store.deleteOperation(op.id);
      });
    }

    result.pushed++;
    _emit(SyncEvent(SyncEventType.operationPushed,
        entity: op.entity, localId: op.localId, message: serverId));
    return true;
  }

  Future<void> _onPushConflict(
    SyncOperation op,
    SyncEntityConfig cfg,
    PushRequest request,
    PushConflict outcome,
    SyncRunResult result,
  ) async {
    final serverRecord = outcome.serverRecord == null
        ? null
        : await _toLocalForm(cfg, outcome.serverRecord!);
    final pending = (await store.operationsFor(op.entity, op.localId))
        .where((o) => !o.isFailed)
        .toList();
    final resolution = await cfg.conflictResolver(SyncConflict(
      entity: op.entity,
      localId: op.localId,
      serverId: request.serverId,
      serverRecord: serverRecord,
      pendingOperations: pending,
      source: ConflictSource.push,
    ));
    _emit(SyncEvent(SyncEventType.conflictResolved,
        entity: op.entity,
        localId: op.localId,
        message: resolution.runtimeType.toString()));

    switch (resolution) {
      case KeepLocal():
        await _backoff(op, outcome.error ?? 'Conflict', result,
            retryAfter: Duration.zero, force: true);
      case MergeResolution(:final merged):
        await store.transaction(() async {
          final serverId = request.serverId;
          if (serverId != null) {
            await cfg.local
                .applyRemote(merged, localId: op.localId, serverId: serverId);
          }
          await _backoff(
              op.copyWith(payload: merged), outcome.error ?? 'Conflict', result,
              retryAfter: Duration.zero, force: true);
        });
      case TakeServer():
        final serverId = request.serverId ??
            (outcome.serverRecord == null
                ? null
                : SyncIds.normalize(cfg.serverIdOf(outcome.serverRecord!)));
        await store.transaction(() async {
          for (final o in pending) {
            await store.deleteOperation(o.id);
          }
          if (serverRecord != null && serverId != null) {
            await cfg.local.applyRemote(serverRecord,
                localId: op.localId, serverId: serverId);
            await store.putMapping(op.entity, op.localId, serverId);
          }
        });
    }
  }

  Future<void> _backoff(
    SyncOperation op,
    Object error,
    SyncRunResult result, {
    Duration? retryAfter,
    bool force = false,
  }) async {
    final attempts = op.attempts + 1;
    if (config.retryPolicy.isExhausted(attempts)) {
      await _markFailed(op.copyWith(attempts: attempts), error, result);
      return;
    }
    final delay = retryAfter ?? config.retryPolicy.delayFor(attempts);
    await store.updateOperation(op.copyWith(
      status: SyncOpStatus.pending,
      attempts: attempts,
      force: force || op.force,
      nextAttemptAt: _now().add(delay),
      lastError: error.toString(),
    ));
  }

  Future<void> _markFailed(
      SyncOperation op, Object error, SyncRunResult result) async {
    final failed = op.copyWith(
      status: SyncOpStatus.failed,
      lastError: error.toString(),
      clearNextAttemptAt: true,
    );
    await store.updateOperation(failed);
    result.failed++;
    result.errors.add('${op.entity}/${op.localId}: $error');
    _emit(SyncEvent(SyncEventType.operationFailed,
        entity: op.entity, localId: op.localId, message: error.toString()));
    _log(SyncLogLevel.warning,
        'Operation ${op.type.name} ${op.entity}/${op.localId} failed: $error');
    final callback = config.onOperationFailed;
    if (callback != null) {
      try {
        callback(failed, error);
      } catch (e, st) {
        _log(SyncLogLevel.error, 'onOperationFailed threw', e, st);
      }
    }
  }

  // Pull ----------------------------------------------------------------------

  Future<void> _pull(SyncRunResult result, Set<String>? only) async {
    for (final name in _pullOrder) {
      if (only != null && !only.contains(name)) continue;
      final cfg = _entities[name]!;
      if (cfg.pullMode == PullMode.none) continue;
      _setCurrent(SyncPhase.pulling, name);
      await _pullEntity(cfg, result);
      if (result.abortedBy != null) return;
    }
  }

  Future<void> _pullEntity(SyncEntityConfig cfg, SyncRunResult result) async {
    final incremental = cfg.pullMode == PullMode.incremental;
    String? cursor =
        incremental ? await store.getMeta(_cursorKey(cfg.name)) : null;
    final seen = <String>{};

    while (true) {
      final PullPage page;
      try {
        page = await cfg.remote.pull(PullRequest(
            entity: cfg.name, cursor: cursor, limit: cfg.pullPageSize));
      } catch (e, st) {
        switch (_classify(e)) {
          case PushNetworkError():
            result.abortedBy = SyncAbortReason.offline;
          case PushUnauthorized():
            result.abortedBy = SyncAbortReason.unauthorized;
          default:
            result.errors.add('Pull ${cfg.name}: $e');
            _log(SyncLogLevel.warning, 'Pull ${cfg.name} failed', e, st);
        }
        return;
      }

      try {
        result.pulled += await store.transaction(
            () => _applyPage(cfg, page, seen, saveCursor: incremental));
      } catch (e, st) {
        result.errors.add('Applying ${cfg.name}: $e');
        _log(SyncLogLevel.error, 'Applying pulled ${cfg.name} failed', e, st);
        return;
      }

      if (!page.hasMore) break;
      if (page.nextCursor == null || page.nextCursor == cursor) {
        // The rest cannot be fetched, so a full refresh must not report the
        // ids seen so far as everything the server has.
        result.errors.add('Pull ${cfg.name}: hasMore without a new cursor');
        return;
      }
      cursor = page.nextCursor;
      // Let the UI breathe between pages.
      await Future<void>.delayed(Duration.zero);
    }

    if (!incremental) {
      try {
        await cfg.local.onFullRefreshComplete(seen);
      } catch (e, st) {
        result.errors.add('Full refresh of ${cfg.name}: $e');
        _log(SyncLogLevel.error, 'onFullRefreshComplete of ${cfg.name} threw',
            e, st);
      }
    }
  }

  /// Applies one pulled page; returns the number of records written.
  Future<int> _applyPage(
    SyncEntityConfig cfg,
    PullPage page,
    Set<String> seen, {
    required bool saveCursor,
  }) async {
    var applied = 0;
    final withOps = await _recordsWithOperations(cfg.name);
    for (final record in page.records) {
      final serverId = SyncIds.normalize(cfg.serverIdOf(record));
      if (serverId == null) continue;
      seen.add(serverId);
      if (cfg.isDeletedOf?.call(record) ?? false) {
        await _applyServerDelete(cfg, serverId);
      } else {
        await _applyServerRecord(cfg, serverId, record, withOps);
        applied++;
      }
    }
    for (final raw in page.deletedServerIds) {
      final serverId = SyncIds.normalize(raw);
      if (serverId != null) await _applyServerDelete(cfg, serverId);
    }
    if (saveCursor && page.nextCursor != null) {
      await store.setMeta(_cursorKey(cfg.name), page.nextCursor);
    }
    return applied;
  }

  Future<Set<String>> _recordsWithOperations(String entity) async {
    final ops = await store.operations();
    return {
      for (final op in ops)
        if (op.entity == entity) op.localId,
    };
  }

  Future<void> _applyServerRecord(
    SyncEntityConfig cfg,
    String serverId,
    Map<String, dynamic> record,
    Set<String> recordsWithOps,
  ) async {
    final localId = await _localIdFor(cfg, serverId);
    final localForm = await _toLocalForm(cfg, record);

    if (localId != null && recordsWithOps.contains(localId)) {
      final pending = await store.operationsFor(cfg.name, localId);
      if (pending.isNotEmpty) {
        if (pending.any((o) => o.status == SyncOpStatus.inFlight)) return;
        final resolution = await cfg.conflictResolver(SyncConflict(
          entity: cfg.name,
          localId: localId,
          serverId: serverId,
          serverRecord: localForm,
          pendingOperations: pending,
          source: ConflictSource.pull,
        ));
        _emit(SyncEvent(SyncEventType.conflictResolved,
            entity: cfg.name,
            localId: localId,
            message: resolution.runtimeType.toString()));
        switch (resolution) {
          case KeepLocal():
            // The local change wins: make sure it overwrites the server.
            for (final o in pending) {
              if (!o.force) {
                await store.updateOperation(o.copyWith(force: true));
              }
            }
            return;
          case MergeResolution(:final merged):
            await cfg.local
                .applyRemote(merged, localId: localId, serverId: serverId);
            for (final o in pending) {
              await store.deleteOperation(o.id);
            }
            final now = _now();
            await store.insertOperation(SyncOperation(
              id: SyncIds.uuid(),
              entity: cfg.name,
              localId: localId,
              type: SyncOpType.update,
              payload: merged,
              force: true,
              createdAt: now,
              updatedAt: now,
            ));
            return;
          case TakeServer():
            for (final o in pending) {
              await store.deleteOperation(o.id);
            }
        }
      }
    }

    final applied = SyncIds.normalize(await cfg.local
        .applyRemote(localForm, localId: localId, serverId: serverId));
    if (applied == null) {
      throw StateError('${cfg.name}: applyRemote must return the local id');
    }
    if (applied != localId) {
      await store.putMapping(cfg.name, applied, serverId);
    }
    _emit(SyncEvent(SyncEventType.recordPulled,
        entity: cfg.name, localId: applied, message: serverId));
  }

  Future<void> _applyServerDelete(SyncEntityConfig cfg, String serverId) async {
    final localId = await _localIdFor(cfg, serverId);
    if (localId == null) return;
    final pending = await store.operationsFor(cfg.name, localId);
    if (pending.isNotEmpty) {
      _log(
          SyncLogLevel.warning,
          '${cfg.name}/$localId was deleted on the server; dropping '
          '${pending.length} unpushed operation(s)');
      for (final o in pending) {
        await store.deleteOperation(o.id);
      }
    }
    // The mapping is kept so references to the record still resolve to the
    // (deleted) server id and fail visibly instead of being guessed.
    await cfg.local.applyRemoteDelete(localId, serverId: serverId);
    _emit(SyncEvent(SyncEventType.recordDeletedByServer,
        entity: cfg.name, localId: localId, message: serverId));
  }

  /// Converts references in a server record to local ids.
  Future<Map<String, dynamic>> _toLocalForm(
      SyncEntityConfig cfg, Map<String, dynamic> record) async {
    var out = record;
    for (final reference in cfg.references) {
      final targetCfg = _entities[reference.target];
      final resolved = <String, String?>{};
      for (final value in reference.read(out)) {
        final serverRef = SyncIds.normalize(value);
        if (serverRef == null || resolved.containsKey(serverRef)) continue;
        resolved[serverRef] = targetCfg == null
            ? (await store.mappingForServer(reference.target, serverRef))
                ?.localId
            : await _localIdFor(targetCfg, serverRef);
      }
      if (resolved.values.any((v) => v != null)) {
        out = reference.rewrite(out, (value) {
          final localRef = resolved[SyncIds.normalize(value)];
          return localRef == null
              ? value
              : SyncIds.toJsonValue(localRef, value);
        });
      }
    }
    return out;
  }

  Future<String?> _serverIdFor(SyncEntityConfig cfg, String localId) async {
    final mapping = await store.mappingForLocal(cfg.name, localId);
    if (mapping != null) return mapping.serverId;
    final legacy = SyncIds.normalize(await cfg.local.findServerId(localId));
    if (legacy != null) await store.putMapping(cfg.name, localId, legacy);
    return legacy;
  }

  Future<String?> _localIdFor(SyncEntityConfig cfg, String serverId) async {
    final mapping = await store.mappingForServer(cfg.name, serverId);
    if (mapping != null) return mapping.localId;
    final legacy = SyncIds.normalize(await cfg.local.findLocalId(serverId));
    if (legacy != null) await store.putMapping(cfg.name, legacy, serverId);
    return legacy;
  }

  // Inspection & maintenance --------------------------------------------------

  /// Operations waiting to be pushed, oldest first.
  Future<List<SyncOperation>> pendingOperations() async {
    await _ensureInit();
    return store
        .operations(statuses: {SyncOpStatus.pending, SyncOpStatus.inFlight});
  }

  /// Operations that need attention.
  Future<List<SyncOperation>> failedOperations() async {
    await _ensureInit();
    return store.operations(statuses: {SyncOpStatus.failed});
  }

  /// Every unpushed operation of one record, oldest first. Use it to show
  /// why a record did not sync yet ([SyncOperation.lastError]).
  Future<List<SyncOperation>> operationsOf(
      String entity, Object localId) async {
    final id = SyncIds.normalize(localId);
    if (id == null) return const [];
    await _ensureInit();
    return store.operationsFor(entity, id);
  }

  /// Puts failed operations (all, or the one with [operationId]) back in the
  /// queue with a fresh attempt budget, then schedules a push.
  Future<void> retryFailed([String? operationId]) async {
    await _ensureInit();
    final failed = await store.operations(statuses: {SyncOpStatus.failed});
    for (final op in failed) {
      if (operationId != null && op.id != operationId) continue;
      // Re-read: the app may have edited the record in the meantime.
      final retried = await store.transaction(() async {
        final fresh = await store.operationById(op.id);
        if (fresh == null || !fresh.isFailed) return false;
        await store.updateOperation(fresh.copyWith(
          status: SyncOpStatus.pending,
          attempts: 0,
          clearNextAttemptAt: true,
          clearLastError: true,
        ));
        return true;
      });
      if (retried) {
        _emit(SyncEvent(SyncEventType.operationRetried,
            entity: op.entity, localId: op.localId, message: op.type.name));
      }
    }
    await _refreshCounts();
    _schedule(pull: false);
  }

  /// Removes an operation for good (for example a bill the user decided not
  /// to keep). If it was the create of a local-only record, records that
  /// reference it will fail with a clear message instead of being sent with
  /// a wrong id.
  Future<void> discard(String operationId) async {
    await _ensureInit();
    final op = await store.operationById(operationId);
    if (op == null) return;
    await store.deleteOperation(operationId);
    _emit(SyncEvent(SyncEventType.operationDiscarded,
        entity: op.entity, localId: op.localId, message: op.type.name));
    await _refreshCounts();
    // Later operations of the record, and records that waited for it, can
    // now go ahead (or fail visibly).
    _schedule(pull: false);
  }

  /// Sync state of one record, for "not synced yet" badges.
  Future<RecordSyncState> recordState(String entity, Object localId) async {
    final id = SyncIds.normalize(localId);
    if (id == null) return RecordSyncState.synced;
    await _ensureInit();
    final ops = await store.operationsFor(entity, id);
    if (ops.any((o) => o.isFailed)) return RecordSyncState.failed;
    if (ops.isNotEmpty) return RecordSyncState.pending;
    return RecordSyncState.synced;
  }

  /// The [RecordSyncState] of one record now and after every change.
  ///
  /// The state is re-read only when an event concerns this record or a run
  /// ends, so watching many rows of a list stays cheap. Equal consecutive
  /// states are not repeated.
  Stream<RecordSyncState> watchRecordState(String entity, Object localId) {
    final id = SyncIds.normalize(localId);
    late final StreamController<RecordSyncState> controller;
    StreamSubscription<SyncEvent>? subscription;
    RecordSyncState? last;
    var generation = 0;

    Future<void> reload() async {
      final current = ++generation;
      try {
        final state = await recordState(entity, localId);
        // Only the latest read counts: an older one may finish later.
        if (current != generation || controller.isClosed) return;
        if (state != last) {
          last = state;
          controller.add(state);
        }
      } catch (e, st) {
        if (!controller.isClosed) controller.addError(e, st);
      }
    }

    controller = StreamController<RecordSyncState>(
      onListen: () {
        subscription = events.listen(
          (event) {
            if (event.type == SyncEventType.runFinished ||
                event.type == SyncEventType.storeCleared ||
                (event.entity == entity && event.localId == id)) {
              reload();
            }
          },
          onDone: () => controller.close(),
        );
        reload();
      },
      onCancel: () => subscription?.cancel(),
    );
    return controller.stream;
  }

  /// Server id of a local record, or null if it has not been pushed.
  Future<String?> serverIdOf(String entity, Object localId) async {
    final id = SyncIds.normalize(localId);
    if (id == null) return null;
    await _ensureInit();
    return _serverIdFor(_config(entity), id);
  }

  /// Local id of a server record, or null if it is not on this device.
  Future<String?> localIdOf(String entity, Object serverId) async {
    final id = SyncIds.normalize(serverId);
    if (id == null) return null;
    await _ensureInit();
    return _localIdFor(_config(entity), id);
  }

  /// Tells the engine that a local row already corresponds to a server row
  /// (for data that existed before the engine was installed, or records
  /// declared with [recordCreatedVia]).
  Future<void> registerMapping(
      String entity, Object localId, Object serverId) async {
    final l = SyncIds.normalize(localId);
    final s = SyncIds.normalize(serverId);
    if (l == null || s == null) {
      throw ArgumentError('Both localId and serverId are required');
    }
    _config(entity);
    await _ensureInit();
    await store.transaction(() async {
      await store.putMapping(entity, l, s);
      await store.setMeta(_viaKey(entity, l), null);
    });
  }

  /// Forgets the pull cursor so the next pull starts from the beginning.
  /// Waits for a run in progress.
  Future<void> resetPullCursor([String? entity]) async {
    if (entity != null) _config(entity);
    await _ensureInit();
    await _exclusive(() async {
      for (final name in entity == null ? _entities.keys : [entity]) {
        await store.setMeta(_cursorKey(name), null);
      }
    });
  }

  /// Wipes outbox, id map and cursors (for logout or a tenant switch).
  /// Unpushed changes are lost; check [status] first.
  ///
  /// Waits for a run in progress, so nothing it was doing is written back
  /// afterwards.
  Future<void> clear() async {
    await _ensureInit();
    await _exclusive(() async {
      _retryTimer?.cancel();
      _retryTimer = null;
      await store.clear();
      _setStatus(SyncStatus(isOnline: _status.value.isOnline));
      await _refreshCounts();
      _emit(const SyncEvent(SyncEventType.storeCleared));
    });
  }

  // Helpers -------------------------------------------------------------------

  SyncEntityConfig _config(String entity) {
    final cfg = _entities[entity];
    if (cfg == null) {
      throw ArgumentError.value(entity, 'entity',
          'is not registered. Registered: ${_entities.keys.join(', ')}');
    }
    return cfg;
  }

  DateTime _now() => (config.clock ?? DateTime.now)();

  String _recordKey(String entity, String localId) => '$entity\u0000$localId';

  String _cursorKey(String entity) => 'cursor:$entity';

  String _viaKey(String entity, String localId) => 'via:$entity\u0000$localId';

  void _setStatus(SyncStatus value) {
    if (!_disposed) _status.value = value;
  }

  void _setCurrent(SyncPhase phase, String entity) =>
      _setStatus(_status.value.copyWith(phase: phase, currentEntity: entity));

  Future<void> _refreshCounts() async {
    if (_disposed) return;
    try {
      final counts = await store.countByStatus();
      _setStatus(_status.value.copyWith(
        pendingCount:
            counts[SyncOpStatus.pending]! + counts[SyncOpStatus.inFlight]!,
        failedCount: counts[SyncOpStatus.failed]!,
      ));
    } catch (e, st) {
      _log(SyncLogLevel.warning, 'Could not count the outbox', e, st);
    }
  }

  void _emit(SyncEvent event) {
    if (!_events.isClosed) _events.add(event);
  }

  void _log(SyncLogLevel level, String message,
      [Object? error, StackTrace? stackTrace]) {
    final logger = config.logger;
    if (logger != null) {
      try {
        logger(level, message, error, stackTrace);
      } catch (_) {
        // A broken logger must not break sync.
      }
    } else if (level.index >= SyncLogLevel.warning.index) {
      developer.log(message,
          name: 'flutter_offline_first_sync',
          level: level == SyncLogLevel.error ? 1000 : 900,
          error: error,
          stackTrace: stackTrace);
    }
  }

  static Map<String, SyncEntityConfig> _index(List<SyncEntityConfig> list) {
    final map = <String, SyncEntityConfig>{};
    for (final cfg in list) {
      if (map.containsKey(cfg.name)) {
        throw ArgumentError('Entity "${cfg.name}" is registered twice');
      }
      map[cfg.name] = cfg;
    }
    for (final cfg in list) {
      for (final dep in cfg.dependencies) {
        if (!map.containsKey(dep)) {
          throw ArgumentError('Entity "${cfg.name}" references "$dep", '
              'which is not registered');
        }
      }
    }
    return map;
  }

  /// Parents before children; registration order otherwise. Cycles are
  /// broken at the edge that closes them.
  static List<String> _dependencyOrder(List<SyncEntityConfig> list) {
    final byName = {for (final cfg in list) cfg.name: cfg};
    final order = <String>[];
    final done = <String>{};
    final visiting = <String>{};
    void visit(String name) {
      if (done.contains(name) || !visiting.add(name)) return;
      for (final dep in byName[name]!.dependencies) {
        visit(dep);
      }
      visiting.remove(name);
      done.add(name);
      order.add(name);
    }

    for (final cfg in list) {
      visit(cfg.name);
    }
    return order;
  }
}

/// Work for one run. Requests made during a run merge into one.
class _RunRequest {
  _RunRequest({required this.push, required this.pull, this.only});

  bool push;
  bool pull;

  /// Entities to sync; null means all of them.
  Set<String>? only;

  final Completer<SyncRunResult> completer = Completer();

  void merge({required bool push, required bool pull, Set<String>? only}) {
    this.push = this.push || push;
    this.pull = this.pull || pull;
    final current = this.only;
    this.only = current == null || only == null ? null : {...current, ...only};
  }
}

/// What happened to one operation during a push pass.
enum _Step {
  /// Accepted by the server.
  pushed,

  /// Left for later (waiting, backoff, failed or aborted).
  held,

  /// A conflict was resolved; the record may be pushed in the next pass.
  resolved,
}

sealed class _Prepared {
  const _Prepared();
}

class _Ready extends _Prepared {
  const _Ready(this.request);
  final PushRequest request;
}

class _Resolved extends _Prepared {
  const _Resolved(this.serverId);
  final String? serverId;
}

class _Wait extends _Prepared {
  const _Wait(this.reason);
  final String reason;
}

class _Reject extends _Prepared {
  const _Reject(this.reason);
  final String reason;
}
