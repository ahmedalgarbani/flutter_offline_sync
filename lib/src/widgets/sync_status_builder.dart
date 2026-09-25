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

/// Shows the [RecordSyncState] of one record and refreshes after each run
/// and each recorded write.
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

  @override
  void initState() {
    super.initState();
    widget.engine.status.addListener(_reload);
    _reload();
  }

  @override
  void didUpdateWidget(RecordSyncStateBuilder oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.engine != widget.engine) {
      oldWidget.engine.status.removeListener(_reload);
      widget.engine.status.addListener(_reload);
    }
    if (oldWidget.engine != widget.engine ||
        oldWidget.entity != widget.entity ||
        oldWidget.localId != widget.localId) {
      _reload();
    }
  }

  @override
  void dispose() {
    widget.engine.status.removeListener(_reload);
    super.dispose();
  }

  Future<void> _reload() async {
    final state =
        await widget.engine.recordState(widget.entity, widget.localId);
    if (mounted && state != _state) setState(() => _state = state);
  }

  @override
  Widget build(BuildContext context) => widget.builder(context, _state);
}
