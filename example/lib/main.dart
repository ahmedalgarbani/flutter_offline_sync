// Minimal, self-contained demo of flutter_offline_sync.
//
// There is no real backend here: a `Map` stands in for the server so the
// example runs with nothing to configure. In a real app, replace the
// `CallbackRemoteAdapter` below with `RestRemoteAdapter` (or your own
// `RemoteAdapter`) and the `CallbackLocalAdapter` with writes into your own
// database (sqflite, drift, Isar, ...).
import 'package:flutter/material.dart';
import 'package:flutter_offline_sync/flutter_offline_sync.dart';

void main() => runApp(const ExampleApp());

class ExampleApp extends StatelessWidget {
  const ExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'flutter_offline_sync example',
      home: const NotesPage(),
    );
  }
}

class NotesPage extends StatefulWidget {
  const NotesPage({super.key});

  @override
  State<NotesPage> createState() => _NotesPageState();
}

class _NotesPageState extends State<NotesPage> {
  /// The app's "local database". A real app would keep this in sqflite,
  /// drift, Isar, ... and call [SyncEngine.recordCreate] etc. in the same
  /// transaction that writes the row.
  final Map<String, String> _notes = {};

  /// The fake "server", so this example needs no network setup.
  final Map<String, String> _serverNotes = {};
  int _nextServerId = 1;

  final _connectivity = ManualConnectivity();
  late final SyncEngine _sync;

  @override
  void initState() {
    super.initState();
    _sync = SyncEngine(
      store: InMemorySyncStore(),
      connectivity: _connectivity,
      config: const SyncConfig(debounce: Duration(milliseconds: 300)),
      entities: [
        SyncEntityConfig(
          name: 'notes',
          // Push-only for this demo; a real backend would also let the
          // engine pull other devices' changes (PullMode.incremental).
          pullMode: PullMode.none,
          remote: CallbackRemoteAdapter(
            push: (request) async {
              // Simulate network latency.
              await Future<void>.delayed(const Duration(milliseconds: 600));
              final text = request.payload['text'] as String;
              switch (request.type) {
                case SyncOpType.create:
                  final serverId = '${_nextServerId++}';
                  _serverNotes[serverId] = text;
                  return PushOutcome.success(serverId: serverId);
                case SyncOpType.update:
                  _serverNotes[request.serverId!] = text;
                  return const PushOutcome.success();
                case SyncOpType.delete:
                  _serverNotes.remove(request.serverId);
                  return const PushOutcome.success();
              }
            },
          ),
          local: CallbackLocalAdapter(
            // Not used with PullMode.none, but every entity needs one.
            applyRemote: (record, {localId, required serverId}) async =>
                localId ?? serverId,
          ),
        ),
      ],
    );
    _sync.start();
  }

  @override
  void dispose() {
    _sync.dispose();
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

  void _deleteNote(String id) {
    setState(() => _notes.remove(id));
    _sync.recordDelete('notes', id);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('flutter_offline_sync example'),
      ),
      body: Column(
        children: [
          SyncStatusBuilder(
            engine: _sync,
            builder: (context, status) => Container(
              width: double.infinity,
              color: status.isUpToDate
                  ? Colors.green.shade100
                  : Colors.orange.shade100,
              padding: const EdgeInsets.all(12),
              child: Text(
                switch (status.phase) {
                  SyncPhase.offline =>
                    'Offline — ${status.pendingCount} change(s) saved locally',
                  _ when status.failedCount > 0 =>
                    '${status.failedCount} change(s) need attention',
                  _ when status.pendingCount > 0 =>
                    'Syncing ${status.pendingCount} change(s)...',
                  _ => 'Everything is synced',
                },
              ),
            ),
          ),
          SwitchListTile(
            title: const Text('Device online'),
            subtitle: const Text('Turn off to see changes queue up offline'),
            value: _connectivity.isOnline,
            onChanged: (value) => setState(() => _connectivity.online = value),
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
                          trailing: Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              RecordSyncStateBuilder(
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
                              IconButton(
                                icon: const Icon(Icons.delete_outline),
                                onPressed: () => _deleteNote(entry.key),
                              ),
                            ],
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
}
