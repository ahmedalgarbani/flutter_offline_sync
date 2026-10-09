import 'package:flutter/widgets.dart';
import 'package:flutter_offline_first_sync/flutter_offline_first_sync.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

void main() {
  late FakeServer server;
  late SyncEngine sync;

  setUp(() {
    server = FakeServer();
    sync = SyncEngine(
      store: InMemorySyncStore(),
      config: manualConfig,
      entities: [entity('notes', server, FakeLocalDb())],
    );
  });

  Widget app(Widget child) =>
      Directionality(textDirection: TextDirection.ltr, child: child);

  testWidgets('SyncStatusBuilder rebuilds when the status changes',
      (tester) async {
    await tester.pumpWidget(app(SyncStatusBuilder(
      engine: sync,
      builder: (context, status) =>
          Text('${status.phase.name} ${status.pendingCount}'),
    )));
    expect(find.text('idle 0'), findsOneWidget);

    await sync.recordCreate('notes', 'a', {});
    await tester.pumpAndSettle();
    expect(find.text('idle 1'), findsOneWidget);

    await sync.syncNow();
    await tester.pumpAndSettle();
    expect(find.text('idle 0'), findsOneWidget);
    await sync.dispose();
  });

  testWidgets('RecordSyncStateBuilder follows its own record', (tester) async {
    server.onPush = (r) =>
        r.localId == 'bad' ? const PushOutcome.rejected('Invalid') : null;
    var builds = 0;
    Widget badge(Object id) => RecordSyncStateBuilder(
          engine: sync,
          entity: 'notes',
          localId: id,
          builder: (context, state) {
            builds++;
            return Text('$id ${state.name}');
          },
        );

    await tester.pumpWidget(app(Column(children: [badge('a'), badge('bad')])));
    await tester.pumpAndSettle();
    expect(find.text('a synced'), findsOneWidget);

    await sync.recordCreate('notes', 'a', {});
    await sync.recordCreate('notes', 'bad', {});
    await tester.pumpAndSettle();
    expect(find.text('a pending'), findsOneWidget);
    expect(find.text('bad pending'), findsOneWidget);

    await sync.syncNow();
    await tester.pumpAndSettle();
    expect(find.text('a synced'), findsOneWidget);
    expect(find.text('bad failed'), findsOneWidget);

    // Changes to other records do not rebuild these badges.
    final before = builds;
    await sync.recordCreate('notes', 'other', {});
    await tester.pumpAndSettle();
    expect(builds, before);

    // Pointing a badge at another record follows that record instead.
    await tester.pumpWidget(app(Column(children: [badge('other')])));
    await tester.pumpAndSettle();
    expect(find.text('other pending'), findsOneWidget);

    await sync.dispose();
    await tester.pumpWidget(const SizedBox());
  });

  group('with an engine started in setUp', () {
    setUp(() => sync.start());
    tearDown(() => sync.dispose());

    // Futures made outside the widget test's fake-async zone must not be
    // waited on again inside it, or the test hangs.
    testWidgets('recording and syncing still complete', (tester) async {
      await sync.recordCreate('notes', 'a', {});
      await sync.syncNow();
      expect(await sync.recordState('notes', 'a'), RecordSyncState.synced);
    });
  });
}
