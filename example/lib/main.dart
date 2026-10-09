// Self-contained demo of flutter_offline_first_sync.
//
// There is no real backend: `FakeServer` below stands in for one so the
// example runs with nothing to configure. In a real app, replace the
// `CallbackRemoteAdapter` with `RestRemoteAdapter` (or your own
// `RemoteAdapter`), and write pulled records into your own database
// (sqflite, drift, ...) in the `CallbackLocalAdapter`.
//
// Try it: add notes while "Device online" is off, then turn it back on;
// make the server reject changes and fix them with Retry; add a note "from
// another device" and pull it with the sync button.
import 'package:flutter/material.dart';
import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';

void main() => runApp(const ExampleApp());

class ExampleApp extends StatelessWidget {
  const ExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return const MaterialApp(
      title: 'flutter_offline_first_sync example',
      home: NotesPage(),
    );
  }
}

class NotesPage extends StatefulWidget {
  const NotesPage({super.key});

  @override
  State<NotesPage> createState() => _NotesPageState();
}

class _NotesPageState extends State<NotesPage> {
  /// The app's "local database": local id -> text. A real app keeps this in
  /// sqflite, drift, ... and calls `recordCreate` etc. in the same
  /// transaction that writes the row.
  final Map<String, String> _notes = {};

  final _server = FakeServer();
  final _connectivity = ManualConnectivity();
  late final SyncEngine _sync;

  @override
  void initState() {
    super.initState();
    _sync = SyncEngine(
      store: InMemorySyncStore(),
      connectivity: _connectivity,
      config: const SyncConfig(
        debounce: Duration(milliseconds: 300),
        retryPolicy: RetryPolicy(maxAttempts: 3),
      ),
      entities: [
        SyncEntityConfig(
          name: 'notes',
          // Resume from the last change seen; deleted notes come back as
          // tombstones.
          pullMode: PullMode.incremental,
          isDeletedOf: (record) => record['deleted'] == true,
          remote: CallbackRemoteAdapter(
            push: _server.push,
            pull: _server.pull,
          ),
          local: CallbackLocalAdapter(
            // References are already converted to local ids here.
            applyRemote: (record, {localId, required serverId}) async {
              final id = localId ?? SyncEngine.newLocalId();
              setState(() => _notes[id] = record['text'] as String);
              return id;
            },
            applyRemoteDelete: (localId, {required serverId}) async =>
                setState(() => _notes.remove(localId)),
          ),
        ),
      ],
    );
    _sync.start();
  }

  @override
  void dispose() {
    _sync.dispose();
    _connectivity.dispose();
    super.dispose();
  }

  void _addNote() {
    final id = SyncEngine.newLocalId();
    final text = 'Note ${_notes.length + 1}';
    setState(() => _notes[id] = text);
    // Record the write; the engine pushes it shortly after (debounced) or
    // as soon as the device comes back online.
    _sync.recordCreate('notes', id, {'text': text});
  }

  void _editNote(String id) {
    final text = '${_notes[id]} ✎';
    setState(() => _notes[id] = text);
    // Merged into the queued create if that has not been pushed yet.
    _sync.recordUpdate('notes', id, {'text': text});
  }

  void _deleteNote(String id) {
    setState(() => _notes.remove(id));
    // Nothing is sent if the note never reached the server.
    _sync.recordDelete('notes', id);
  }

  Future<void> _discardFailed() async {
    for (final op in await _sync.failedOperations()) {
      await _sync.discard(op.id);
    }
  }

  void _addFromAnotherDevice() {
    _server.addFromAnotherDevice('Written on another device');
    ScaffoldMessenger.of(context).showSnackBar(const SnackBar(
        content: Text('Added on the server. Tap sync to pull it.')));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Offline-first notes'),
        actions: [
          IconButton(
            tooltip: 'Add a note on another device',
            icon: const Icon(Icons.devices_other),
            onPressed: _addFromAnotherDevice,
          ),
          IconButton(
            tooltip: 'Sync now',
            icon: const Icon(Icons.sync),
            onPressed: _sync.syncNow,
          ),
        ],
      ),
      body: Column(
        children: [
          SyncStatusBuilder(engine: _sync, builder: _statusBanner),
          SwitchListTile(
            title: const Text('Device online'),
            subtitle: const Text('Turn off to see changes queue up offline'),
            value: _connectivity.isOnline,
            onChanged: (value) => setState(() => _connectivity.online = value),
          ),
          SwitchListTile(
            title: const Text('Server rejects changes'),
            subtitle: const Text('Failed changes are kept until you act'),
            value: _server.rejectChanges,
            onChanged: (value) => setState(() => _server.rejectChanges = value),
          ),
          const Divider(height: 1),
          Expanded(
            child: _notes.isEmpty
                ? const Center(child: Text('No notes yet — tap + to add one'))
                : ListView(
                    children: [
                      for (final entry in _notes.entries)
                        ListTile(
                          title: Text(entry.value),
                          onTap: () => _editNote(entry.key),
                          leading: RecordSyncStateBuilder(
                            engine: _sync,
                            entity: 'notes',
                            localId: entry.key,
                            builder: (context, state) => switch (state) {
                              RecordSyncState.synced => const Icon(
                                  Icons.cloud_done,
                                  color: Colors.green,
                                ),
                              RecordSyncState.pending => const Icon(
                                  Icons.cloud_upload,
                                  color: Colors.orange,
                                ),
                              RecordSyncState.failed => const Icon(
                                  Icons.error,
                                  color: Colors.red,
                                ),
                            },
                          ),
                          trailing: IconButton(
                            icon: const Icon(Icons.delete_outline),
                            onPressed: () => _deleteNote(entry.key),
                          ),
                        ),
                    ],
                  ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: _addNote,
        child: const Icon(Icons.add),
      ),
    );
  }

  Widget _statusBanner(BuildContext context, SyncStatus status) {
    final (text, color) = switch (status.phase) {
      SyncPhase.offline => (
          'Offline — ${status.pendingCount} change(s) saved locally',
          Colors.grey.shade300
        ),
      _ when status.failedCount > 0 => (
          '${status.failedCount} change(s) need attention',
          Colors.red.shade100
        ),
      _ when status.isSyncing => ('Syncing…', Colors.blue.shade100),
      _ when status.nextRetryAt != null => (
          '${status.pendingCount} change(s) waiting to be retried',
          Colors.orange.shade100
        ),
      _ when status.pendingCount > 0 => (
          '${status.pendingCount} change(s) to upload',
          Colors.orange.shade100
        ),
      _ => ('Everything is synced', Colors.green.shade100),
    };
    return Container(
      width: double.infinity,
      color: color,
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: Row(
        children: [
          Expanded(child: Text(text)),
          if (status.failedCount > 0) ...[
            TextButton(
              onPressed: () => _sync.retryFailed(),
              child: const Text('Retry'),
            ),
            TextButton(
              onPressed: _discardFailed,
              child: const Text('Discard'),
            ),
          ],
        ],
      ),
    );
  }
}

/// A pretend backend. Every change gets a version number, and a pull asks
/// for the changes after the last version it saw.
class FakeServer {
  final Map<String, Map<String, dynamic>> _notes = {};
  int _version = 0;
  int _nextId = 1;

  /// Simulates validation errors.
  bool rejectChanges = false;

  Future<PushOutcome> push(PushRequest request) async {
    await Future<void>.delayed(const Duration(milliseconds: 500));
    if (rejectChanges) {
      return const PushOutcome.rejected('The server rejected this change');
    }
    switch (request.type) {
      case SyncOpType.create:
        final id = '${_nextId++}';
        _save(id, request.payload);
        return PushOutcome.success(serverId: id);
      case SyncOpType.update:
        _save(request.serverId!, request.payload);
        return const PushOutcome.success();
      case SyncOpType.delete:
        _notes[request.serverId!] = {
          'id': request.serverId,
          'deleted': true,
          'version': ++_version,
        };
        return const PushOutcome.success();
    }
  }

  Future<PullPage> pull(PullRequest request) async {
    await Future<void>.delayed(const Duration(milliseconds: 300));
    final since = int.tryParse(request.cursor ?? '') ?? 0;
    final changes = _notes.values
        .where((note) => (note['version'] as int) > since)
        .toList()
      ..sort((a, b) => (a['version'] as int).compareTo(b['version'] as int));
    final page = changes.take(request.limit).toList();
    return PullPage(
      records: page,
      hasMore: changes.length > page.length,
      nextCursor: page.isEmpty ? request.cursor : '${page.last['version']}',
    );
  }

  void addFromAnotherDevice(String text) =>
      _save('${_nextId++}', {'text': text});

  void _save(String id, Map<String, dynamic> data) {
    _notes[id] = {
      ...?_notes[id],
      ...data,
      'id': id,
      'deleted': false,
      'version': ++_version,
    };
  }
}
