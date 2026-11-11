import 'dart:async';
import 'dart:developer' as developer;
import 'dart:math' as math;

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';

import '../adapters/local_adapter.dart';
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
///
/// The engine is built for large data sets. It never loads the whole outbox
/// or id map: the outbox is walked a page at a time, pulled pages are looked
/// up and written in batches, and operations can be sent in batches or in
/// parallel (`SyncEntityConfig.pushBatchSize`). Memory use and the cost of
/// each record stay the same whether the device holds a thousand rows or
/// millions.
class SyncEngine with WidgetsBindingObserver {
  SyncEngine({
    required this.store,
    required List<SyncEntityConfig> entities,
    ConnectivitySource? connectivity,
    this.config = const SyncConfig(),
  })  : connectivity = connectivity ?? const AlwaysOnline(),
        _entities = _index(entities),
        _home = Zone.current {
    _pullOrder = _dependencyOrder(entities);
    _outboxPage = entities.fold(config.outboxPageSize,
        (size, cfg) => math.max(size, cfg.pushBatchSize));
  }

  final SyncStore store;
  final ConnectivitySource connectivity;
  final SyncConfig config;

  final Map<String, SyncEntityConfig> _entities;
  late final List<String> _pullOrder;

  /// Operations read from the outbox at a time: at least one full batch.
  late final int _outboxPage;

  /// The zone [start] was called in (the one the engine was created in,
  /// until then). The engine's timers, and so the syncs it starts by
  /// itself, live there and never in the zone of whoever recorded a change:
  /// that is often a database transaction, which is over by the time the
  /// timer fires. A sync started from it would send its statements to a
  /// transaction that no longer exists.
  Zone _home;

  final ValueNotifier<SyncStatus> _status = ValueNotifier(const SyncStatus());
  final StreamController<SyncEvent> _events = StreamController.broadcast();
  final Set<StreamController<SyncStatus>> _statusWatchers = {};

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

  /// Started by the first request a debounced sync is waiting for.
  Stopwatch? _debounceWatch;

  /// Whether `SyncConfig.onUnauthorized` was already called in this run.
  bool _authRetried = false;

  /// Pushes in a row that the server answered "unavailable" in this run.
  int _unavailableInRow = 0;

  /// Times in a row a run was stopped because the server was unavailable,
  /// and until when automatic runs leave it alone.
  int _unavailableStops = 0;
  DateTime? _serverBusyUntil;

  /// A pull was put off because the server was unavailable.
  bool _owesPull = false;

  /// The run that was asked to stop (see [cancelSync]).
  _RunRequest? _cancelled;

  /// Ids of the operations this engine marked in flight and has not
  /// settled yet.
  final Set<String> _claimed = {};

  /// Outbox size at the last count, and when it was made (see
  /// [_refreshCounts]).
  int _countedOperations = 0;
  final Stopwatch _sinceCount = Stopwatch();
  Timer? _countTimer;

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

  /// The current [SyncStatus], then every change, as a stream: for Bloc,
  /// Riverpod (`StreamProvider`), GetX, MobX or any stream-based state
  /// management. Closes when the engine is disposed.
  Stream<SyncStatus> watchStatus() {
    late final StreamController<SyncStatus> controller;
    void notify() {
      if (!controller.isClosed) controller.add(_status.value);
    }

    controller = StreamController<SyncStatus>(
      onListen: () {
        controller.add(_status.value);
        if (_disposed) {
          controller.close();
          return;
        }
        _status.addListener(notify);
        _statusWatchers.add(controller);
      },
      onCancel: () {
        if (_statusWatchers.remove(controller) && !_disposed) {
          _status.removeListener(notify);
        }
      },
    );
    return controller.stream;
  }

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

  /// Whether the run in progress should stop at the next safe point.
  bool get _stopRequested =>
      _disposed || (_cancelled != null && identical(_cancelled, _active));

  // Lifecycle -----------------------------------------------------------------

  /// Prepares the store and installs the automatic triggers.
  ///
  /// If the store fails to open, the error is rethrown and [start] can be
  /// called again.
  Future<void> start() async {
    if (_started || _disposed) return;
    _started = true;
    _home = Zone.current;
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
      _periodic =
          _home.createPeriodicTimer(interval, (_) => _schedule(pull: true));
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

  /// Stops triggers. A run in progress stops after the operations or the
  /// page it is working on (nothing is left half done); work queued behind
  /// it is dropped.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _debounceTimer?.cancel();
    _periodic?.cancel();
    _retryTimer?.cancel();
    _countTimer?.cancel();
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
    final watchers = _statusWatchers.toList();
    _statusWatchers.clear();
    for (final watcher in watchers) {
      // Not awaited: a paused listener would hold dispose up.
      unawaited(watcher.close());
    }
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
    await _refreshCounts(force: true);
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
        // A record created here that never got a server id has nothing to
        // delete on the server, even when its create was discarded. Records
        // created by another push (recordCreatedVia) still wait for it.
        final localOnly = (mapping == null || mapping.isLocalOnly) &&
            (existing.any((o) => o.type == SyncOpType.create) ||
                (mapping != null &&
                    await store.getMeta(_viaKey(entity, id)) == null));
        final neverReachedServer = localOnly &&
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
  ///
  /// Operations waiting out a backoff delay after a temporary failure are
  /// skipped until the delay ends. Pass [ignoreBackoff] for a sync the user
  /// asked for (a "Sync now" button), to try them again at once.
  Future<SyncRunResult> syncNow({
    bool? pull,
    Set<String>? entities,
    bool ignoreBackoff = false,
  }) =>
      _request(
          push: true,
          pull: pull ?? config.pullOnSync,
          only: entities,
          ignoreBackoff: ignoreBackoff);

  /// Pushes without pulling. See [syncNow] for [ignoreBackoff].
  Future<SyncRunResult> pushNow({
    Set<String>? entities,
    bool ignoreBackoff = false,
  }) =>
      _request(
          push: true,
          pull: false,
          only: entities,
          ignoreBackoff: ignoreBackoff);

  /// Pulls without pushing first. Prefer [syncNow], which pushes first so
  /// the server already has local changes.
  Future<SyncRunResult> pullNow({Set<String>? entities}) =>
      _request(push: false, pull: true, only: entities);

  Future<SyncRunResult> _request({
    required bool push,
    required bool pull,
    Set<String>? only,
    bool ignoreBackoff = false,
  }) {
    if (_disposed) {
      return Future.value(SyncRunResult()..abortedBy = SyncAbortReason.paused);
    }
    _RunRequest request() => _RunRequest(
        push: push, pull: pull, only: only, ignoreBackoff: ignoreBackoff);
    if (_running == null && _holds == 0) return _startRun(request());
    final queued = _queued;
    if (queued != null) {
      queued.merge(
          push: push, pull: pull, only: only, ignoreBackoff: ignoreBackoff);
      return queued.completer.future;
    }
    final next = request();
    _queued = next;
    return next.completer.future;
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
      if (identical(_cancelled, request)) _cancelled = null;
      _startQueued();
    });
    return future;
  }

  /// Stops the run in progress after the operations or the page it is
  /// working on, and drops the follow-up run queued behind it. Completes
  /// when the run has stopped; its result reports
  /// [SyncAbortReason.cancelled].
  ///
  /// Nothing is lost or left half done: operations already sent are
  /// recorded, the others stay in the outbox, and an incremental pull
  /// resumes from the last page it saved. Use it when the user leaves a
  /// long first sync, or before signing out ([clear] does it for you).
  Future<void> cancelSync() async {
    final queued = _queued;
    _queued = null;
    queued?.completer
        .complete(SyncRunResult()..abortedBy = SyncAbortReason.cancelled);
    final running = _running;
    if (running == null) return;
    _cancelled = _active;
    if (!_inRun) await running;
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
    // Each request restarts the debounce, but never past maxDebounce from
    // the first one: a steady stream of writes must not starve sync (the
    // periodic and reconnect triggers come through here too).
    final waited = (_debounceWatch ??= Stopwatch()..start()).elapsed;
    var delay = immediate ? Duration.zero : config.debounce;
    final maxDelay = config.maxDebounce;
    if (maxDelay != null && waited + delay > maxDelay) {
      delay = waited >= maxDelay ? Duration.zero : maxDelay - waited;
    }
    _debounceTimer = _home.createTimer(delay, () {
      _debounceWatch = null;
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
    _authRetried = false;
    _unavailableInRow = 0;
    _emit(const SyncEvent(SyncEventType.runStarted));
    _setStatus(_status.value.copyWith(runPushed: 0, runPulled: 0));
    try {
      await _ensureInit();
      final gate = config.canSync;
      if (gate != null && !await gate()) {
        result.abortedBy = SyncAbortReason.paused;
      } else if (!connectivity.isOnline) {
        result.abortedBy = SyncAbortReason.offline;
      } else if (request.push && !request.ignoreBackoff && _serverIsBusy) {
        // The server asked to be left alone for a while.
        result.abortedBy = SyncAbortReason.serverUnavailable;
      } else {
        if (request.push) {
          if (request.ignoreBackoff) {
            await store.clearBackoff(_now(), entities: request.only);
          }
          await _push(result, request.only);
        }
        if (result.abortedBy == null && request.pull) {
          await _pull(result, request.only);
        }
      }
    } catch (e, st) {
      crashed = true;
      result.errors.add(e.toString());
      _log(SyncLogLevel.error, 'Sync run failed', e, st);
    }
    await _releaseClaimed();
    stopwatch.stop();
    result.duration = stopwatch.elapsed;

    final serverUnavailable =
        result.abortedBy == SyncAbortReason.serverUnavailable;
    // A pull the pause kept from happening is made when the pause ends.
    if (request.pull) _owesPull = serverUnavailable;

    final nextRetry = await _scheduleRetry();
    if (!_disposed) {
      final phase = switch (result.abortedBy) {
        SyncAbortReason.offline => SyncPhase.offline,
        SyncAbortReason.unauthorized => SyncPhase.authRequired,
        SyncAbortReason.paused => SyncPhase.paused,
        SyncAbortReason.serverUnavailable => SyncPhase.serverUnavailable,
        SyncAbortReason.cancelled || null => SyncPhase.idle,
      };
      // A run the pause stopped before it began has no error of its own:
      // the one that started the pause still explains the state.
      final keepError = serverUnavailable && result.errors.isEmpty;
      _setStatus(_status.value.copyWith(
        phase: phase,
        isOnline: result.abortedBy == SyncAbortReason.offline
            ? false
            : connectivity.isOnline,
        clearCurrentEntity: true,
        lastSyncedAt: result.completed && !crashed ? _now() : null,
        lastError: result.errors.isEmpty ? null : result.errors.last,
        clearLastError: result.errors.isEmpty && !keepError,
        nextRetryAt: nextRetry,
        clearNextRetryAt: nextRetry == null,
        runPushed: result.pushed,
        runPulled: result.pulled,
      ));
      await _refreshCounts(force: true);
    }
    _emit(SyncEvent(SyncEventType.runFinished, message: result.toString()));
    _log(SyncLogLevel.info, 'Sync run finished: $result');
    return result;
  }

  bool get _serverIsBusy {
    final until = _serverBusyUntil;
    return until != null && until.isAfter(_now());
  }

  /// Arms a timer for the earliest operation waiting out a backoff delay
  /// (or for the end of the pause an unavailable server caused), so it is
  /// retried without waiting for another trigger. Returns that time.
  Future<DateTime?> _scheduleRetry() async {
    _retryTimer?.cancel();
    _retryTimer = null;
    if (_disposed) return null;
    DateTime? earliest;
    try {
      earliest = await store.nextAttemptAfter(_now());
    } catch (e, st) {
      _log(SyncLogLevel.warning, 'Could not read the outbox', e, st);
      return null;
    }
    final now = _now();
    // Nothing is sent before the pause ends, and operations that were not
    // tried yet are due as soon as it does.
    final busyUntil = _serverBusyUntil;
    if (busyUntil != null && busyUntil.isAfter(now)) earliest = busyUntil;
    if (earliest == null || _disposed) return null;
    if (_started) {
      _retryTimer = _home.createTimer(earliest.difference(now),
          () => _schedule(pull: _owesPull, immediate: true));
    }
    return earliest;
  }

  // Push ----------------------------------------------------------------------

  Future<void> _push(SyncRunResult result, Set<String>? only) async {
    for (var pass = 0; pass < config.maxPushPasses; pass++) {
      final progress = await _pushPass(result, only);
      if (result.abortedBy != null || !progress) break;
    }
    result.waiting = await store.countOperations(
        statuses: const {SyncOpStatus.pending}, entities: only);
  }

  /// One ordered walk over the operations that are due, a page of the
  /// outbox at a time. Returns true if anything changed that could unblock
  /// other operations.
  Future<bool> _pushPass(SyncRunResult result, Set<String>? only) async {
    var progress = false;
    var afterSeq = 0;
    while (result.abortedBy == null) {
      final page = await store.operationsPage(
        statuses: const {SyncOpStatus.pending},
        entities: only,
        dueAt: _now(),
        afterSeq: afterSeq,
        limit: _outboxPage,
      );
      var index = 0;
      while (index < page.length && result.abortedBy == null) {
        if (_stopRequested) {
          result.abortedBy = SyncAbortReason.cancelled;
          break;
        }
        final first = page[index++];
        final cfg = _entities[first.entity];
        if (cfg == null) {
          await _markFailed(first, 'Unknown entity "${first.entity}"', result);
          continue;
        }
        // Operations that follow each other, of one entity and of different
        // records, are sent together (one at a time by default).
        final chunk = <SyncOperation>[first];
        final records = <String>{first.localId};
        while (index < page.length &&
            chunk.length < cfg.pushBatchSize &&
            page[index].entity == cfg.name &&
            records.add(page[index].localId)) {
          chunk.add(page[index++]);
        }
        if (await _pushChunk(chunk, cfg, result)) progress = true;
        await _refreshCounts();
      }
      if (page.length < _outboxPage) break;
      afterSeq = page.last.seq!;
    }
    return progress;
  }

  /// Claims, prepares, sends and settles [chunk]: operations of [cfg], each
  /// of another record. Returns true if anything changed that could unblock
  /// other operations. Leaves nothing claimed.
  Future<bool> _pushChunk(
    List<SyncOperation> chunk,
    SyncEntityConfig cfg,
    SyncRunResult result,
  ) async {
    final claimed = await _claim(cfg.name, chunk);
    if (claimed.isEmpty) return false;
    _setCurrent(SyncPhase.pushing, cfg.name);

    final mappings = _Mappings(store);
    if (claimed.length > 1) await _preloadMappings(mappings, cfg, claimed);

    var progress = false;
    final group = <_Ready>[];
    for (var i = 0; i < claimed.length; i++) {
      final op = claimed[i];
      var prepared = await _prepareOrBackOff(op, cfg, mappings, result);
      if (prepared is _Wait && group.isNotEmpty) {
        // It may be waiting for an operation of this very group (a record
        // that references another one of its own entity): send the group,
        // then look again.
        if (await _sendGroup(List.of(group), cfg, result)) progress = true;
        group.clear();
        mappings.clear();
        if (result.abortedBy != null) {
          await _releaseAll(claimed.sublist(i));
          return progress;
        }
        prepared = await _prepareOrBackOff(op, cfg, mappings, result);
      }
      switch (prepared) {
        case _Ready():
          group.add(prepared);
        case _Wait(:final reason):
          await _release(op, reason);
        case _Reject(:final reason):
          await _markFailed(op, reason, result);
        case _Resolved() || null:
          // Preparing failed and the operation was put back with a delay.
          break;
      }
    }
    if (group.isNotEmpty && await _sendGroup(group, cfg, result)) {
      progress = true;
    }
    return progress;
  }

  /// Re-reads and claims operations of [entity], each of another record:
  /// the app may have merged new data into them, or removed them, since the
  /// page was loaded.
  ///
  /// An operation is left alone while an earlier one of its record is still
  /// in the outbox (failed, waiting for something or backing off): the
  /// changes of a record reach the server in the order they were made.
  Future<List<SyncOperation>> _claim(
          String entity, List<SyncOperation> listed) =>
      store.transaction(() async {
        final fresh = listed.length == 1
            ? await store.operationsFor(entity, listed.single.localId)
            : await store.operationsForRecords(
                entity, [for (final op in listed) op.localId]);
        final firstOf = <String, SyncOperation>{};
        for (final op in fresh) {
          firstOf.putIfAbsent(op.localId, () => op);
        }
        final now = _now();
        final claimed = <SyncOperation>[];
        for (final op in listed) {
          final first = firstOf[op.localId];
          if (first == null || first.id != op.id || !first.isPending) continue;
          final next = first.nextAttemptAt;
          if (next != null && next.isAfter(now)) continue;
          claimed.add(first.copyWith(status: SyncOpStatus.inFlight));
        }
        if (claimed.isNotEmpty) {
          final ids = [for (final op in claimed) op.id];
          await store.markOperations(ids, SyncOpStatus.inFlight);
          _claimed.addAll(ids);
        }
        return claimed;
      });

  /// Reads the id mappings a group of operations needs in one go: their own
  /// and those of the records they reference.
  Future<void> _preloadMappings(
    _Mappings mappings,
    SyncEntityConfig cfg,
    List<SyncOperation> operations,
  ) async {
    await mappings.load(cfg.name, [for (final op in operations) op.localId]);
    for (final reference in cfg.references) {
      final ids = <String>{};
      for (final op in operations) {
        for (final value in reference.read(op.payload)) {
          final id = SyncIds.normalize(value);
          if (id != null) ids.add(id);
        }
      }
      await mappings.load(reference.target, ids);
    }
  }

  /// [_prepare], or null when it threw and the operation was put back with
  /// a delay.
  Future<_Prepared?> _prepareOrBackOff(
    SyncOperation op,
    SyncEntityConfig cfg,
    _Mappings mappings,
    SyncRunResult result,
  ) async {
    try {
      return await _prepare(op, cfg, mappings);
    } catch (e, st) {
      // A local failure, e.g. buildPushPayload threw: retry with backoff.
      _log(SyncLogLevel.warning, 'Preparing ${op.entity}/${op.localId} failed',
          e, st);
      await _backoff(op, e, result);
      return null;
    }
  }

  /// Sends the prepared operations of [group] and settles each of them.
  /// Returns true if anything changed that could unblock other operations.
  /// Never leaves an operation in flight unless the store itself fails.
  Future<bool> _sendGroup(
    List<_Ready> group,
    SyncEntityConfig cfg,
    SyncRunResult result,
  ) async {
    final outcomes = await _send(cfg, group);
    var progress = false;

    // Accepted operations first, recorded together.
    final accepted = <_Accepted>[];
    for (var i = 0; i < group.length; i++) {
      final outcome = outcomes[i];
      if (outcome is! PushSuccess) continue;
      final request = group[i].request;
      var serverId = request.serverId;
      if (request.type == SyncOpType.create) {
        serverId = SyncIds.normalize(outcome.serverId) ??
            (outcome.record == null
                ? null
                : SyncIds.normalize(cfg.serverIdOf(outcome.record!)));
        if (serverId == null) {
          // Retrying would create a duplicate, so stop and surface it.
          await _markFailed(
              group[i].op,
              'The server accepted the create but returned no id. '
              'Return PushOutcome.success(serverId: ...) from the adapter.',
              result);
          continue;
        }
      }
      accepted.add(_Accepted(group[i].op, request, outcome, serverId));
    }
    if (accepted.isNotEmpty) {
      await _recordAccepted(cfg, accepted, result);
      progress = true;
      // The server is answering.
      _unavailableInRow = 0;
      _unavailableStops = 0;
      _serverBusyUntil = null;
    }

    var authRefreshed = false;
    for (var i = 0; i < group.length; i++) {
      final op = group[i].op;
      final outcome = outcomes[i];
      switch (outcome) {
        case PushSuccess():
          break;
        case PushNetworkError(:final error):
          await _release(op, error.toString());
          result.abortedBy ??= SyncAbortReason.offline;
        case PushUnauthorized(:final error):
          await _release(op, (error ?? 'Unauthorized').toString());
          // A refreshed session retries the operation in the next pass.
          if (authRefreshed || await _refreshAuth()) {
            authRefreshed = true;
            progress = true;
          } else {
            result.abortedBy ??= SyncAbortReason.unauthorized;
          }
        case PushRetry(:final error, :final retryAfter):
          await _backoff(op, error, result, retryAfter: retryAfter);
          if (outcome.serverUnavailable) {
            _onServerUnavailable(error, retryAfter, result);
          } else {
            _unavailableInRow = 0;
          }
        case PushRejected(:final error):
          _unavailableInRow = 0;
          await _markFailed(op, error, result);
        case PushConflict():
          _unavailableInRow = 0;
          try {
            await _onPushConflict(op, cfg, group[i].request, outcome, result);
            progress = true;
          } catch (e, st) {
            _log(
                SyncLogLevel.warning,
                'Resolving the conflict on ${op.entity}/${op.localId} failed',
                e,
                st);
            await _backoff(op, e, result);
          }
      }
    }
    if (accepted.isNotEmpty) {
      _setStatus(_status.value.copyWith(runPushed: result.pushed));
    }
    return progress;
  }

  /// Sends [group] and returns one outcome per operation, in order. Thrown
  /// errors become outcomes through `SyncConfig.classifyError`.
  Future<List<PushOutcome>> _send(
      SyncEntityConfig cfg, List<_Ready> group) async {
    Future<PushOutcome> one(PushRequest request) async {
      try {
        return await cfg.remote.push(request);
      } catch (e, st) {
        _log(SyncLogLevel.warning,
            'Push ${request.entity}/${request.localId} threw', e, st);
        return _classify(e);
      }
    }

    final requests = [for (final ready in group) ready.request];
    if (cfg.pushBatchSize <= 1) return [await one(requests.single)];

    final List<PushOutcome>? outcomes;
    try {
      outcomes = await cfg.remote.pushBatch(requests);
    } catch (e, st) {
      _log(SyncLogLevel.warning,
          'Pushing ${requests.length} ${cfg.name} operations threw', e, st);
      return List.filled(requests.length, _classify(e));
    }
    // No batch endpoint: send the operations side by side.
    if (outcomes == null) return Future.wait(requests.map(one));
    if (outcomes.length != requests.length) {
      final error = StateError(
          '${cfg.name}: pushBatch returned ${outcomes.length} outcomes for '
          '${requests.length} requests; it must return one per request, in '
          'the same order');
      _log(SyncLogLevel.error, error.message);
      return List.filled(requests.length, PushOutcome.retry(error));
    }
    return outcomes;
  }

  /// Counts a push the server answered "unavailable" (overloaded, down for
  /// maintenance, rate limited). After `SyncConfig.serverUnavailableThreshold`
  /// of them in a row the run stops, as it does when the network is gone:
  /// sending the rest of the outbox would only add load to a server that is
  /// struggling, and use up the attempts of every operation.
  ///
  /// Automatic runs then stay away for the retry policy's delay (or the
  /// server's `Retry-After`), which grows each time the server is still
  /// unavailable. After a pause one such answer is enough to stop again.
  void _onServerUnavailable(
      Object error, Duration? retryAfter, SyncRunResult result) {
    _unavailableInRow++;
    final threshold =
        _unavailableStops > 0 ? 1 : config.serverUnavailableThreshold;
    if (threshold == null ||
        _unavailableInRow < threshold ||
        result.abortedBy != null) {
      return;
    }
    _unavailableStops++;
    var delay = config.retryPolicy.delayFor(_unavailableStops);
    if (retryAfter != null && retryAfter > delay) delay = retryAfter;
    _serverBusyUntil = _now().add(delay);
    result.abortedBy = SyncAbortReason.serverUnavailable;
    final message = 'The server is unavailable ($error); '
        'trying again in ${_describeDelay(delay)}';
    result.errors.add(message);
    _log(SyncLogLevel.warning, message);
  }

  static String _describeDelay(Duration delay) => delay.inSeconds < 120
      ? '${math.max(1, delay.inSeconds)} s'
      : '${delay.inMinutes} min';

  /// Puts a claimed operation back in the queue without counting an
  /// attempt.
  Future<void> _release(SyncOperation op, String reason) {
    _claimed.remove(op.id);
    return store.updateOperation(
        op.copyWith(status: SyncOpStatus.pending, lastError: reason));
  }

  /// Puts claimed operations nothing was done with back in the queue.
  Future<void> _releaseAll(List<SyncOperation> operations) {
    final ids = [for (final op in operations) op.id];
    _claimed.removeAll(ids);
    return store.markOperations(ids, SyncOpStatus.pending);
  }

  /// Puts back whatever a run that failed midway (a store error) left
  /// claimed, so those operations do not wait for the next app start.
  Future<void> _releaseClaimed() async {
    if (_claimed.isEmpty) return;
    final ids = _claimed.toList();
    _claimed.clear();
    try {
      await store.transaction(() async {
        // Only those still in flight: the others were settled.
        final stuck = <String>[];
        for (final id in ids) {
          final op = await store.operationById(id);
          if (op != null && op.status == SyncOpStatus.inFlight) stuck.add(id);
        }
        await store.markOperations(stuck, SyncOpStatus.pending);
      });
    } catch (e, st) {
      _log(SyncLogLevel.warning,
          'Could not put ${ids.length} claimed operation(s) back', e, st);
    }
  }

  /// Gives the app one chance per run to refresh expired credentials.
  Future<bool> _refreshAuth() async {
    final refresh = config.onUnauthorized;
    if (refresh == null || _authRetried) return false;
    _authRetried = true;
    try {
      return await refresh();
    } catch (e, st) {
      _log(SyncLogLevel.warning, 'onUnauthorized threw', e, st);
      return false;
    }
  }

  PushOutcome _classify(Object error) {
    try {
      return config.classifyError(error);
    } catch (e, st) {
      _log(SyncLogLevel.error, 'classifyError threw', e, st);
      return PushOutcome.retry(error);
    }
  }

  Future<_Prepared> _prepare(
      SyncOperation op, SyncEntityConfig cfg, _Mappings mappings) async {
    var type = op.type;
    final serverId = await _serverIdFor(cfg, op.localId, mappings);
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
            reference.target, localRef, cfg.unknownReferencePolicy, mappings);
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
    return _Ready(
      op,
      PushRequest(operation: sendOp, payload: payload, serverId: serverId),
    );
  }

  Future<_Prepared> _resolveOutbound(
    String target,
    String localRef,
    UnknownReferencePolicy policy,
    _Mappings mappings,
  ) async {
    final mapping = await mappings.forLocal(target, localRef);
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
      mappings.put(target, localRef, legacy);
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

  /// Records what the server accepted: the new ids, the app's own
  /// bookkeeping, and the removal from the outbox.
  ///
  /// A group is recorded in one transaction, which is what makes a batch
  /// cheap. If the app's bookkeeping fails for one record, that undoes the
  /// others too, so they are then recorded one by one.
  Future<void> _recordAccepted(
    SyncEntityConfig cfg,
    List<_Accepted> accepted,
    SyncRunResult result,
  ) async {
    var recorded = false;
    if (accepted.length > 1) {
      try {
        await store.transaction(() async {
          await store.putMappings(cfg.name, {
            for (final a in accepted)
              if (a.isCreate) a.op.localId: a.serverId,
          });
          await store.deleteOperations([for (final a in accepted) a.op.id]);
          for (final a in accepted) {
            if (a.isCreate) {
              await cfg.local.onServerIdAssigned(a.op.localId, a.serverId!,
                  serverRecord: a.outcome.record);
            }
            await cfg.local.onPushed(a.op, serverId: a.serverId);
          }
        });
        recorded = true;
      } catch (e, st) {
        _log(
            SyncLogLevel.warning,
            'Recording ${accepted.length} pushed ${cfg.name} operations '
            'together failed; recording them one by one',
            e,
            st);
      }
    }
    for (final a in accepted) {
      if (!recorded) await _recordOneAccepted(cfg, a, result);
      _claimed.remove(a.op.id);
      result.pushed++;
      _emit(SyncEvent(SyncEventType.operationPushed,
          entity: a.op.entity, localId: a.op.localId, message: a.serverId));
    }
  }

  Future<void> _recordOneAccepted(
      SyncEntityConfig cfg, _Accepted a, SyncRunResult result) async {
    final op = a.op;
    try {
      await store.transaction(() async {
        if (a.isCreate) {
          await store.putMapping(op.entity, op.localId, a.serverId);
          await cfg.local.onServerIdAssigned(op.localId, a.serverId!,
              serverRecord: a.outcome.record);
        }
        await store.deleteOperation(op.id);
        await cfg.local.onPushed(op, serverId: a.serverId);
      });
    } catch (e, st) {
      // The server has the change: never send it again, even though the
      // app's own bookkeeping failed.
      _log(SyncLogLevel.error,
          'Recording the push of ${op.entity}/${op.localId} failed', e, st);
      result.errors.add('${op.entity}/${op.localId}: $e');
      await store.transaction(() async {
        if (a.isCreate) {
          await store.putMapping(op.entity, op.localId, a.serverId);
        }
        await store.deleteOperation(op.id);
      });
    }
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
            immediate: true, force: true);
      case MergeResolution(:final merged):
        await store.transaction(() async {
          // The updates queued behind this one were part of the conflict's
          // localPayload, so [merged] already decided their fields: sending
          // them afterwards would undo the merge.
          for (final o in pending) {
            if (o.id != op.id &&
                o.type == SyncOpType.update &&
                (o.seq ?? 0) > (op.seq ?? 0)) {
              await store.deleteOperation(o.id);
            }
          }
          final serverId = request.serverId;
          if (serverId != null) {
            await cfg.local
                .applyRemote(merged, localId: op.localId, serverId: serverId);
          }
          await _backoff(
              op.copyWith(payload: merged), outcome.error ?? 'Conflict', result,
              immediate: true, force: true);
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
        _claimed.remove(op.id);
    }
  }

  /// Counts an attempt and puts [op] back in the queue after a delay: the
  /// retry policy's backoff, or the server's [retryAfter] when that is
  /// longer. A `Retry-After` of zero (or a date in the past, as a skewed
  /// clock produces) must not resend at once and burn every attempt in one
  /// run. [immediate] resends in the next pass (after a conflict was
  /// resolved in favour of the local copy).
  Future<void> _backoff(
    SyncOperation op,
    Object error,
    SyncRunResult result, {
    Duration? retryAfter,
    bool immediate = false,
    bool force = false,
  }) async {
    final attempts = op.attempts + 1;
    if (config.retryPolicy.isExhausted(attempts)) {
      await _markFailed(op.copyWith(attempts: attempts), error, result);
      return;
    }
    var delay =
        immediate ? Duration.zero : config.retryPolicy.delayFor(attempts);
    if (!immediate && retryAfter != null && retryAfter > delay) {
      delay = retryAfter;
    }
    _claimed.remove(op.id);
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
    _claimed.remove(op.id);
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
      if (_stopRequested) {
        result.abortedBy = SyncAbortReason.cancelled;
        return;
      }
      _setCurrent(SyncPhase.pulling, name);
      await _pullEntity(cfg, result);
      if (result.abortedBy != null) return;
    }
  }

  Future<void> _pullEntity(SyncEntityConfig cfg, SyncRunResult result) async {
    final incremental = cfg.pullMode == PullMode.incremental;
    String? cursor =
        incremental ? await store.getMeta(_cursorKey(cfg.name)) : null;
    // Only a full refresh reports the ids it saw; an incremental pull of
    // millions of records must not keep them all in memory.
    final seen = incremental ? null : <String>{};

    while (true) {
      if (_stopRequested) {
        result.abortedBy = SyncAbortReason.cancelled;
        return;
      }
      final PullPage page;
      try {
        page = await cfg.remote.pull(PullRequest(
            entity: cfg.name, cursor: cursor, limit: cfg.pullPageSize));
      } catch (e, st) {
        switch (_classify(e)) {
          case PushNetworkError():
            result.abortedBy = SyncAbortReason.offline;
          case PushUnauthorized():
            // A refreshed session requests the same page again.
            if (await _refreshAuth()) continue;
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
      _setStatus(_status.value.copyWith(runPulled: result.pulled));

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

    if (seen != null) {
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
  ///
  /// The page is handled as a whole: one lookup tells which of its records
  /// are already on the device, one per referenced entity converts the
  /// references, one finds the records with unpushed changes, and the rows
  /// are handed to `LocalAdapter.applyRemoteBatch` together. The number of
  /// database statements depends on the number of pages, not of records.
  /// Records are still applied in the order the server sent them.
  Future<int> _applyPage(
    SyncEntityConfig cfg,
    PullPage page,
    Set<String>? seen, {
    required bool saveCursor,
  }) async {
    final name = cfg.name;
    final serverIds = <String>[];
    final records = <Map<String, dynamic>>[];
    for (final record in page.records) {
      final serverId = SyncIds.normalize(cfg.serverIdOf(record));
      if (serverId == null) continue;
      serverIds.add(serverId);
      records.add(record);
      seen?.add(serverId);
    }
    final deleted = <String>[
      for (final raw in page.deletedServerIds)
        if (SyncIds.normalize(raw) case final serverId?) serverId,
    ];

    // Local ids by entity and server id; null when the device has none.
    final local = <String, Map<String, String?>>{};
    final known =
        local[name] = await _localIdsFor(cfg, name, [...serverIds, ...deleted]);
    await _loadReferences(cfg, records, local);

    // Records this device created that still wait for their server id: the
    // server may be sending one of them back (see [SyncEntityConfig.clientIdOf]).
    final clientIdOf = cfg.clientIdOf;
    final unconfirmed = <String>{};
    if (clientIdOf != null) {
      final candidates = <String>{};
      for (var i = 0; i < records.length; i++) {
        if (known[serverIds[i]] != null) continue;
        final clientId = SyncIds.normalize(clientIdOf(records[i]));
        if (clientId != null) candidates.add(clientId);
      }
      if (candidates.isNotEmpty) {
        final mappings = await store.mappingsForLocal(name, candidates);
        for (final mapping in mappings.values) {
          if (mapping.isLocalOnly) unconfirmed.add(mapping.localId);
        }
      }
    }

    final withOps = await store.localIdsWithOperations(
        name, {...known.values.whereType<String>(), ...unconfirmed});
    final selfReferences = [
      for (final reference in cfg.references)
        if (reference.target == name) reference,
    ];

    var applied = 0;
    // Records waiting to be written together, and their server ids.
    final buffer = <RemoteChange>[];
    final buffered = <String>{};

    Future<void> flush() async {
      if (buffer.isEmpty) return;
      final changes = List<RemoteChange>.of(buffer);
      buffer.clear();
      buffered.clear();
      final localIds = await cfg.local.applyRemoteBatch(changes);
      if (localIds.length != changes.length) {
        throw StateError('$name: applyRemoteBatch returned ${localIds.length} '
            'local ids for ${changes.length} records; it must return one '
            'per record, in the same order');
      }
      final mapped = <String, String?>{};
      for (var i = 0; i < changes.length; i++) {
        final change = changes[i];
        final localId = SyncIds.normalize(localIds[i]);
        if (localId == null) {
          throw StateError('$name: applyRemote must return the local id');
        }
        if (localId != change.localId) mapped[localId] = change.serverId;
        known[change.serverId] = localId;
      }
      await store.putMappings(name, mapped);
      applied += changes.length;
      if (_events.hasListener) {
        for (final change in changes) {
          _emit(SyncEvent(SyncEventType.recordPulled,
              entity: name,
              localId: known[change.serverId],
              message: change.serverId));
        }
      }
    }

    for (var i = 0; i < records.length; i++) {
      final serverId = serverIds[i];
      final record = records[i];
      // The same record twice in a page: the first copy goes in first.
      if (buffered.contains(serverId)) await flush();

      if (cfg.isDeletedOf?.call(record) ?? false) {
        await flush();
        await _applyServerDelete(cfg, serverId, known[serverId], withOps);
        continue;
      }

      var localId = known[serverId];
      if (localId == null && clientIdOf != null) {
        final clientId = SyncIds.normalize(clientIdOf(record));
        if (clientId != null && unconfirmed.remove(clientId)) {
          // Created here; the create reached the server but its answer was
          // lost. Link the two instead of storing a second copy.
          await store.putMapping(name, clientId, serverId);
          await cfg.local
              .onServerIdAssigned(clientId, serverId, serverRecord: record);
          _log(SyncLogLevel.info,
              'Pulled $name/$serverId is local record $clientId; linked them');
          known[serverId] = localId = clientId;
        }
      }

      // A record that references one of its own entity which is still
      // waiting to be written needs that one's local id first.
      if (buffered.isNotEmpty) {
        for (final reference in selfReferences) {
          if (reference
              .read(record)
              .any((value) => buffered.contains(SyncIds.normalize(value)))) {
            await flush();
            break;
          }
        }
      }
      final localForm = _localForm(cfg, record, local);

      if (localId != null && withOps.contains(localId)) {
        await flush();
        switch (await _resolvePullConflict(cfg, serverId, localId, localForm)) {
          case _PullStep.skip:
            continue;
          case _PullStep.written:
            applied++;
            continue;
          case _PullStep.apply:
            break;
        }
      }
      buffer.add(RemoteChange(
          record: localForm, localId: localId, serverId: serverId));
      buffered.add(serverId);
    }
    await flush();

    for (final serverId in deleted) {
      await _applyServerDelete(cfg, serverId, known[serverId], withOps);
    }
    if (saveCursor && page.nextCursor != null) {
      await store.setMeta(_cursorKey(name), page.nextCursor);
    }
    return applied;
  }

  /// Decides what happens to a pulled record whose local copy has unpushed
  /// changes.
  Future<_PullStep> _resolvePullConflict(
    SyncEntityConfig cfg,
    String serverId,
    String localId,
    Map<String, dynamic> localForm,
  ) async {
    final pending = await store.operationsFor(cfg.name, localId);
    if (pending.isEmpty) return _PullStep.apply;
    if (pending.any((o) => o.status == SyncOpStatus.inFlight)) {
      return _PullStep.skip;
    }
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
        return _PullStep.skip;
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
        return _PullStep.written;
      case TakeServer():
        for (final o in pending) {
          await store.deleteOperation(o.id);
        }
        return _PullStep.apply;
    }
  }

  /// Removes the local copy of a record the server deleted. [localId] is
  /// its local id, if the device has it; [withOps] the local ids known to
  /// have operations in the outbox.
  Future<void> _applyServerDelete(
    SyncEntityConfig cfg,
    String serverId,
    String? localId,
    Set<String> withOps,
  ) async {
    if (localId == null) return;
    if (withOps.contains(localId)) {
      final pending = await store.operationsFor(cfg.name, localId);
      if (pending.isNotEmpty) {
        _log(
            SyncLogLevel.warning,
            '${cfg.name}/$localId was deleted on the server; dropping '
            '${pending.length} unpushed operation(s)');
        for (final o in pending) {
          await store.deleteOperation(o.id);
          _emit(SyncEvent(SyncEventType.operationDiscarded,
              entity: cfg.name,
              localId: localId,
              message: '${o.type.name}: the record was deleted on the server'));
        }
      }
    }
    // The mapping is kept so references to the record still resolve to the
    // (deleted) server id and fail visibly instead of being guessed.
    await cfg.local.applyRemoteDelete(localId, serverId: serverId);
    _emit(SyncEvent(SyncEventType.recordDeletedByServer,
        entity: cfg.name, localId: localId, message: serverId));
  }

  /// The local id of each of [serverIds] of [entity], null for those the
  /// device does not have: one lookup in the id map, then
  /// `LocalAdapter.findLocalId` for the ones it does not hold.
  Future<Map<String, String?>> _localIdsFor(
    SyncEntityConfig? cfg,
    String entity,
    Iterable<String> serverIds,
  ) async {
    final wanted = serverIds.toSet();
    final out = <String, String?>{};
    if (wanted.isEmpty) return out;
    final found = await store.localIdsForServer(entity, wanted);
    for (final serverId in wanted) {
      var localId = found[serverId];
      if (localId == null && cfg != null) {
        localId = SyncIds.normalize(await cfg.local.findLocalId(serverId));
        if (localId != null) await store.putMapping(entity, localId, serverId);
      }
      out[serverId] = localId;
    }
    return out;
  }

  /// Adds to [local] the local ids of everything [records] of [cfg]
  /// reference, one lookup per referenced entity.
  Future<void> _loadReferences(
    SyncEntityConfig cfg,
    List<Map<String, dynamic>> records,
    Map<String, Map<String, String?>> local,
  ) async {
    for (final reference in cfg.references) {
      final target = local.putIfAbsent(reference.target, () => {});
      final wanted = <String>{};
      for (final record in records) {
        for (final value in reference.read(record)) {
          final serverRef = SyncIds.normalize(value);
          if (serverRef != null && !target.containsKey(serverRef)) {
            wanted.add(serverRef);
          }
        }
      }
      if (wanted.isEmpty) continue;
      target.addAll(await _localIdsFor(
          _entities[reference.target], reference.target, wanted));
    }
  }

  /// [record] with its references converted to the local ids in [local].
  /// The record itself is returned when nothing had to change.
  Map<String, dynamic> _localForm(
    SyncEntityConfig cfg,
    Map<String, dynamic> record,
    Map<String, Map<String, String?>> local,
  ) {
    Map<String, dynamic>? copy;
    for (final reference in cfg.references) {
      final resolved = local[reference.target];
      if (resolved == null) continue;
      if (!reference
          .read(copy ?? record)
          .any((value) => resolved[SyncIds.normalize(value)] != null)) {
        continue;
      }
      // Copied once, however many references it has.
      copy ??= deepCopyJson(record);
      reference.rewriteInPlace(copy, (value) {
        final localRef = resolved[SyncIds.normalize(value)];
        return localRef == null ? value : SyncIds.toJsonValue(localRef, value);
      });
    }
    return copy ?? record;
  }

  /// Converts references in one server record to local ids.
  Future<Map<String, dynamic>> _toLocalForm(
      SyncEntityConfig cfg, Map<String, dynamic> record) async {
    final local = <String, Map<String, String?>>{};
    await _loadReferences(cfg, [record], local);
    return _localForm(cfg, record, local);
  }

  Future<String?> _serverIdFor(SyncEntityConfig cfg, String localId,
      [_Mappings? mappings]) async {
    final mapping = mappings == null
        ? await store.mappingForLocal(cfg.name, localId)
        : await mappings.forLocal(cfg.name, localId);
    if (mapping != null) return mapping.serverId;
    final legacy = SyncIds.normalize(await cfg.local.findServerId(localId));
    if (legacy != null) {
      await store.putMapping(cfg.name, localId, legacy);
      mappings?.put(cfg.name, localId, legacy);
    }
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
  ///
  /// With a large outbox, read it a page at a time: pass [limit], then the
  /// `seq` of the last operation of a page as [afterSeq] for the next one.
  /// `status.value.pendingCount` gives the total without loading anything.
  Future<List<SyncOperation>> pendingOperations({
    int? limit,
    int afterSeq = 0,
  }) =>
      _listOperations(
          const {SyncOpStatus.pending, SyncOpStatus.inFlight}, limit, afterSeq);

  /// Operations that need attention, oldest first. [limit] and [afterSeq]
  /// page through them as in [pendingOperations].
  Future<List<SyncOperation>> failedOperations({
    int? limit,
    int afterSeq = 0,
  }) =>
      _listOperations(const {SyncOpStatus.failed}, limit, afterSeq);

  Future<List<SyncOperation>> _listOperations(
      Set<SyncOpStatus> statuses, int? limit, int afterSeq) async {
    await _ensureInit();
    if (limit == null && afterSeq <= 0) {
      return store.operations(statuses: statuses);
    }
    return store.operationsPage(
        statuses: statuses,
        afterSeq: afterSeq,
        // No limit: everything after [afterSeq].
        limit: limit ?? 0x7fffffff);
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
    // A page at a time, each in one transaction.
    var afterSeq = 0;
    while (true) {
      final List<SyncOperation> page;
      if (operationId != null) {
        final op = await store.operationById(operationId);
        page = [if (op != null) op];
      } else {
        page = await store.operationsPage(
            statuses: const {SyncOpStatus.failed},
            afterSeq: afterSeq,
            limit: _outboxPage);
      }
      if (page.isEmpty) break;
      final retried = await store.transaction(() async {
        final done = <SyncOperation>[];
        for (final listed in page) {
          // Re-read: the app may have edited the record in the meantime.
          final fresh = await store.operationById(listed.id);
          if (fresh == null || !fresh.isFailed) continue;
          await store.updateOperation(fresh.copyWith(
            status: SyncOpStatus.pending,
            attempts: 0,
            clearNextAttemptAt: true,
            clearLastError: true,
          ));
          done.add(fresh);
        }
        return done;
      });
      for (final op in retried) {
        _emit(SyncEvent(SyncEventType.operationRetried,
            entity: op.entity, localId: op.localId, message: op.type.name));
      }
      if (operationId != null || page.length < _outboxPage) break;
      afterSeq = page.last.seq!;
    }
    await _refreshCounts(force: true);
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
    await _refreshCounts(force: true);
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

  /// [registerMapping] for many records of one [entity] at once, in a single
  /// transaction: for migrating an app whose rows already carry server ids.
  ///
  /// ```dart
  /// await sync.registerMappings('customers', {
  ///   for (final row in rows) if (row.serverId != null) row.id: row.serverId!,
  /// });
  /// ```
  Future<void> registerMappings(
      String entity, Map<Object, Object> localToServer) async {
    _config(entity);
    final pairs = <String, String?>{};
    localToServer.forEach((localId, serverId) {
      final l = SyncIds.normalize(localId);
      final s = SyncIds.normalize(serverId);
      if (l == null || s == null) {
        throw ArgumentError.value(
            '$localId -> $serverId', 'localToServer', 'holds an empty id');
      }
      pairs[l] = s;
    });
    if (pairs.isEmpty) return;
    await _ensureInit();
    await store.transaction(() async {
      // Only a record declared with [recordCreatedVia] has a link to
      // forget, and such a record is known as local-only until now.
      final before = await store.mappingsForLocal(entity, pairs.keys);
      await store.putMappings(entity, pairs);
      for (final mapping in before.values) {
        if (mapping.isLocalOnly) {
          await store.setMeta(_viaKey(entity, mapping.localId), null);
        }
      }
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
  /// A run in progress is stopped first (see [cancelSync]) and waited for,
  /// so signing out never waits for a long sync to finish, and nothing the
  /// run was doing is written back afterwards.
  Future<void> clear() async {
    await _ensureInit();
    if (!_inRun) await cancelSync();
    await _exclusive(() async {
      _retryTimer?.cancel();
      _retryTimer = null;
      _serverBusyUntil = null;
      _unavailableStops = 0;
      _owesPull = false;
      await store.clear();
      _setStatus(SyncStatus(isOnline: _status.value.isOnline));
      await _refreshCounts(force: true);
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

  String _cursorKey(String entity) => 'cursor:$entity';

  String _viaKey(String entity, String localId) => 'via:$entity\u0000$localId';

  void _setStatus(SyncStatus value) {
    if (!_disposed) _status.value = value;
  }

  void _setCurrent(SyncPhase phase, String entity) =>
      _setStatus(_status.value.copyWith(phase: phase, currentEntity: entity));

  /// Below this many operations the outbox is counted after every change.
  static const _countEveryTimeBelow = 2000;

  /// Updates the counts in [status].
  ///
  /// Counting reads an index of the whole outbox. A small outbox is counted
  /// after every change; a large one at intervals that grow with its size
  /// (0.1 s at 20 000 operations, 5 s at a million), or recording and
  /// pushing a million operations would count a million rows a million
  /// times. The count is always exact when a run ends ([force]).
  Future<void> _refreshCounts({bool force = false}) async {
    if (_disposed) return;
    if (!force && _countedOperations > _countEveryTimeBelow) {
      final wait =
          Duration(microseconds: _countedOperations * 5) - _sinceCount.elapsed;
      if (wait > Duration.zero) {
        _countTimer ??= _home.createTimer(wait, () {
          _countTimer = null;
          _refreshCounts(force: true);
        });
        return;
      }
    }
    _countTimer?.cancel();
    _countTimer = null;
    try {
      final counts = await store.countByStatus();
      final pending =
          counts[SyncOpStatus.pending]! + counts[SyncOpStatus.inFlight]!;
      final failed = counts[SyncOpStatus.failed]!;
      _countedOperations = pending + failed;
      _sinceCount
        ..reset()
        ..start();
      _setStatus(
          _status.value.copyWith(pendingCount: pending, failedCount: failed));
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
  _RunRequest({
    required this.push,
    required this.pull,
    this.only,
    this.ignoreBackoff = false,
  });

  bool push;
  bool pull;

  /// Entities to sync; null means all of them.
  Set<String>? only;

  /// Retry operations in backoff now (a sync the user asked for).
  bool ignoreBackoff;

  final Completer<SyncRunResult> completer = Completer();

  void merge({
    required bool push,
    required bool pull,
    Set<String>? only,
    bool ignoreBackoff = false,
  }) {
    this.push = this.push || push;
    this.pull = this.pull || pull;
    this.ignoreBackoff = this.ignoreBackoff || ignoreBackoff;
    final current = this.only;
    this.only = current == null || only == null ? null : {...current, ...only};
  }
}

/// What to do with a pulled record whose local copy has unpushed changes.
enum _PullStep {
  /// The local copy stays as it is.
  skip,

  /// The resolver's merge was written.
  written,

  /// The server's version is written like any other pulled record.
  apply,
}

/// The id mappings read while preparing a group of operations, so each is
/// looked up once.
class _Mappings {
  _Mappings(this.store);

  final SyncStore store;

  /// By entity and local id; a null value means "no mapping".
  final Map<String, Map<String, IdMapping?>> _known = {};

  /// Reads the mappings of [localIds] of [entity] in one lookup.
  Future<void> load(String entity, Iterable<String> localIds) async {
    final known = _known.putIfAbsent(entity, () => {});
    final wanted = {
      for (final id in localIds)
        if (!known.containsKey(id)) id,
    };
    if (wanted.isEmpty) return;
    final found = await store.mappingsForLocal(entity, wanted);
    for (final id in wanted) {
      known[id] = found[id];
    }
  }

  Future<IdMapping?> forLocal(String entity, String localId) async {
    final known = _known.putIfAbsent(entity, () => {});
    if (known.containsKey(localId)) return known[localId];
    return known[localId] = await store.mappingForLocal(entity, localId);
  }

  void put(String entity, String localId, String serverId) =>
      _known.putIfAbsent(entity, () => {})[localId] =
          IdMapping(entity: entity, localId: localId, serverId: serverId);

  /// Forgets everything: mappings changed (operations were pushed).
  void clear() => _known.clear();
}

/// An operation the server accepted.
class _Accepted {
  const _Accepted(this.op, this.request, this.outcome, this.serverId);

  final SyncOperation op;
  final PushRequest request;
  final PushSuccess outcome;

  /// The record's server id: the new one for a create.
  final String? serverId;

  bool get isCreate => request.type == SyncOpType.create;
}

sealed class _Prepared {
  const _Prepared();
}

class _Ready extends _Prepared {
  const _Ready(this.op, this.request);

  /// The claimed operation, as stored.
  final SyncOperation op;
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
