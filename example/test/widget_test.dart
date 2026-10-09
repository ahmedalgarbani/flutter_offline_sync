import 'package:flutter/material.dart';
import 'package:flutter_offline_first_sync_example/main.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  /// Debounce, simulated latency of the push and of the pull that follows.
  Future<void> syncs(WidgetTester tester) async {
    await tester.pump(const Duration(seconds: 2));
    await tester.pump();
  }

  testWidgets('notes sync, survive offline time and failures', (tester) async {
    await tester.pumpWidget(const ExampleApp());
    await syncs(tester);
    expect(find.text('Everything is synced'), findsOneWidget);

    // Create, then delete: both reach the server.
    await tester.tap(find.byIcon(Icons.add));
    await tester.pump(); // the row
    await tester.pump(); // its badge, once the record state is read
    expect(find.text('Note 1'), findsOneWidget);
    expect(find.byIcon(Icons.cloud_upload), findsOneWidget);
    await syncs(tester);
    expect(find.byIcon(Icons.cloud_done), findsOneWidget);
    expect(find.text('Everything is synced'), findsOneWidget);

    await tester.tap(find.byIcon(Icons.delete_outline));
    await syncs(tester);
    expect(find.text('Note 1'), findsNothing);
    expect(find.text('Everything is synced'), findsOneWidget);

    // Offline: the change waits, then goes out on reconnect.
    await tester.tap(find.text('Device online'));
    await tester.tap(find.byIcon(Icons.add));
    await syncs(tester);
    expect(find.text('Offline — 1 change(s) saved locally'), findsOneWidget);
    await tester.tap(find.text('Device online'));
    await syncs(tester);
    expect(find.text('Everything is synced'), findsOneWidget);

    // Rejected by the server: kept, shown, and retried after a fix.
    await tester.tap(find.text('Server rejects changes'));
    await tester.tap(find.text('Note 1'));
    await syncs(tester);
    expect(find.byIcon(Icons.error), findsOneWidget);
    expect(find.text('1 change(s) need attention'), findsOneWidget);
    await tester.tap(find.text('Server rejects changes'));
    await tester.tap(find.text('Retry'));
    await syncs(tester);
    expect(find.byIcon(Icons.cloud_done), findsOneWidget);
    expect(find.text('Everything is synced'), findsOneWidget);

    // A note written elsewhere arrives with the next pull.
    await tester.tap(find.byTooltip('Add a note on another device'));
    await tester.tap(find.byTooltip('Sync now'));
    await syncs(tester);
    expect(find.text('Written on another device'), findsOneWidget);
  });
}
