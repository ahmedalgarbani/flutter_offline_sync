import 'dart:async';

import 'package:flutter/widgets.dart';

import '../core/status.dart';
import '../engine/sync_engine.dart';

/// Rebuilds when the engine's [SyncStatus] changes.
///
/// ```dart
/// SyncStatusBuilder(
///   engine: sync,
///   builder: (context, status) => status.isUpToDate
///       ? const Icon(Icons.cloud_done)
///       : Badge(label: Text('${status.pendingCount}'),
///               child: const Icon(Icons.cloud_upload)),
/// )
/// ```
class SyncStatusBuilder extends StatelessWidget {
  const SyncStatusBuilder({
    super.key,
    required this.engine,
    required this.builder,
  });

  final SyncEngine engine;
  final Widget Function(BuildContext context, SyncStatus status) builder;

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<SyncStatus>(
      valueListenable: engine.status,
      builder: (context, status, _) => builder(context, status),
    );
  }
}

/// Shows the [RecordSyncState] of one record and keeps it current.
///
/// Built on [SyncEngine.watchRecordState]: the state is re-read only when an
/// event concerns this record or a run ends, so it is cheap to use on every
/// row of a long list.
class RecordSyncStateBuilder extends StatefulWidget {
  const RecordSyncStateBuilder({
    super.key,
    required this.engine,
    required this.entity,
    required this.localId,
    required this.builder,
  });

  final SyncEngine engine;
  final String entity;
  final Object localId;
  final Widget Function(BuildContext context, RecordSyncState state) builder;

  @override
  State<RecordSyncStateBuilder> createState() => _RecordSyncStateBuilderState();
}

class _RecordSyncStateBuilderState extends State<RecordSyncStateBuilder> {
  RecordSyncState _state = RecordSyncState.synced;
  StreamSubscription<RecordSyncState>? _subscription;

  @override
  void initState() {
    super.initState();
    _subscribe();
  }

  @override
  void didUpdateWidget(RecordSyncStateBuilder oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.engine != widget.engine ||
        oldWidget.entity != widget.entity ||
        oldWidget.localId != widget.localId) {
      _subscription?.cancel();
      _subscribe();
    }
  }

  void _subscribe() {
    _subscription = widget.engine
        .watchRecordState(widget.entity, widget.localId)
        .listen((state) {
      if (mounted && state != _state) setState(() => _state = state);
    }, onError: (Object error, StackTrace stackTrace) {
      // Keep the last known state; the next event reads it again.
    });
  }

  @override
  void dispose() {
    _subscription?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.builder(context, _state);
}
