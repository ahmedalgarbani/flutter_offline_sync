import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('records a create and pushes it to get a server id', () async {
    final server = <String, Map<String, dynamic>>{};
    var nextServerId = 1;

    final sync = SyncEngine(
      store: InMemorySyncStore(),
      entities: [
        SyncEntityConfig(
          name: 'notes',
          pullMode: PullMode.none,
          remote: CallbackRemoteAdapter(
            push: (request) async {
              switch (request.type) {
                case SyncOpType.create:
                  final id = '${nextServerId++}';
                  server[id] = request.payload;
                  return PushOutcome.success(serverId: id);
                case SyncOpType.update:
                  server[request.serverId!] = request.payload;
                  return const PushOutcome.success();
                case SyncOpType.delete:
                  server.remove(request.serverId);
                  return const PushOutcome.success();
              }
            },
          ),
          local: CallbackLocalAdapter(
            applyRemote: (record, {localId, required serverId}) async =>
                localId ?? serverId,
          ),
        ),
      ],
    );
    addTearDown(sync.dispose);
    await sync.start();

    final localId = SyncEngine.newLocalId();
    await sync.recordCreate('notes', localId, {'text': 'hello'});
    await sync.syncNow();

    final serverId = await sync.serverIdOf('notes', localId);
    expect(serverId, isNotNull);
    expect(server[serverId]?['text'], 'hello');
    expect(sync.status.value.isUpToDate, isTrue);
  });
}
