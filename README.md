# flutter_offline_first_sync

[![pub package](https://img.shields.io/pub/v/flutter_offline_first_sync.svg)](https://pub.dev/packages/flutter_offline_first_sync)
[![license](https://img.shields.io/github/license/ahmedalgarbani/flutter_offline_sync.svg)](LICENSE)

An **offline-first** sync engine for Flutter that works with the database and
backend you already have.

Your app **always reads and writes its local database**. `flutter_offline_first_sync`
records every change in a durable outbox and sends it to the server when
possible, in the right order, exactly once. It also pulls server changes down
without overwriting edits that have not been pushed yet.

- **Dependency-aware push** — a bill never reaches the server before its
  customer, nor a customer before its account. Local ids inside references are
  rewritten to server ids automatically, in a single sync.
- **No guessing** — a local id with no server id yet is never sent as if it
  were one. A record either waits for its parent or fails with a clear
  message.
- **No data loss** — failed operations are kept, never dropped, until you
  retry, fix the data, or discard them. Time spent offline never burns a retry
  attempt, and temporary failures are retried by themselves once their
  backoff delay ends.
- **Exactly-once delivery** via an idempotency key sent with every operation,
  plus "already exists → adopt" handling.
- **Coalescing** — create then update collapses into a single create; create
  then delete sends nothing at all.
- **Resumable pull** — page by page, with a saved cursor. Parent tables are
  pulled first, server ids are converted to local ids, and conflicts go
  through a resolver you can customize.
- **Automatic sync** — after writes (briefly debounced to batch bursts), on
  reconnect, when the app returns to the foreground, periodically, when a
  backoff delay ends, and on demand. Only one sync run at a time; requests
  made during a run are merged into a single follow-up run, so nothing is
  lost.
- **UI-ready state** — a `ValueListenable<SyncStatus>`, per-record state
  streams, and an event stream.
- **No mandatory dependencies** — use drift, sqflite, or any SQLite for
  storage, and `http`, `dio`, or anything else for the network.

## How it works

```
 UI ──reads/writes──▶ your local DB ──(same transaction)──▶ sync outbox
                            ▲                                   │
                            │ applyRemote            push, in order,
                            │ (pulled records)       ids rewritten
                            │                                   ▼
                        SyncEngine ◀────────── pull pages ──── server
```

For each entity (table) you provide:

| Part | Responsibility |
|---|---|
| `RemoteAdapter` | Pushes one change and pulls one page. `RestRemoteAdapter` covers most REST APIs. |
| `LocalAdapter` | Writes a pulled record into your table and receives server ids. |
| `SyncReference` | Declares which fields point at which entity (`customerId → customers`, `items[].itemId → items`). |

## Getting started

```dart
final sync = SyncEngine(
  store: SqlSyncStore(DriftSqlExecutor(db)), // or InMemorySyncStore()
  connectivity: StreamConnectivity(
    initial: await InternetConnection().hasInternetAccess,
    changes: InternetConnection().onStatusChange
        .map((s) => s == InternetStatus.connected),
  ),
  config: SyncConfig(canSync: () => session.isValid),
  entities: [
    SyncEntityConfig(
      name: 'accounts',
      remote: RestRemoteAdapter(transport: transport, resource: 'accounts'),
      local: AccountsLocal(db),
      pullMode: PullMode.fullRefresh,
    ),
    SyncEntityConfig(
      name: 'customers',
      remote: RestRemoteAdapter(transport: transport, resource: 'customers'),
      local: CustomersLocal(db),
      references: [SyncReference('accountId', 'accounts')],
    ),
    SyncEntityConfig(
      name: 'bills',
      remote: RestRemoteAdapter(transport: transport, resource: 'bills'),
      local: BillsLocal(db),
      references: [
        SyncReference('customerId', 'customers'),
        SyncReference('items[].itemId', 'items'),
      ],
    ),
    // ...
  ],
);
await sync.start();
```

See [`example/`](example) for a complete, runnable app (no backend required —
it simulates one in memory).

Record every local write inside the same transaction that writes the row:

```dart
await db.transaction(() async {
  final id = await db.into(db.customers).insert(row);
  await sync.recordCreate('customers', id, customer.toJson());
});

await sync.recordUpdate('customers', id, {'phone': '0100...'}); // changed fields are enough
await sync.recordDelete('customers', id);
```

That's it. The engine pushes the change moments later, or as soon as
connectivity returns.

### Reference fields hold local ids

Inside your database, and in the data you record, a reference field always
holds the **local id** of the referenced record. The engine converts it to a
server id when pushing, and server ids back to local ids when pulling. Never
store a server id in a reference column: mixing the two is exactly what makes
records attach to the wrong parent.

The safest local ids are UUIDs from `SyncEngine.newLocalId()`, but
auto-incrementing integers work too. A server id written into a reference
keeps the JSON type of the local value it replaces; with string (UUID) local
ids and a server that expects numbers, set `serverIdToJson: int.parse` on the
referenced entity:

```dart
SyncEntityConfig(name: 'customers', serverIdToJson: int.parse, /* ... */)
```

## Showing status

```dart
SyncStatusBuilder(
  engine: sync,
  builder: (context, s) => switch (s.phase) {
    SyncPhase.offline => const Text('Offline – changes are saved locally'),
    SyncPhase.authRequired => const Text('Session expired, please sign in again'),
    _ when s.failedCount > 0 => Text('${s.failedCount} operation(s) need review'),
    _ when s.pendingCount > 0 => Text('${s.pendingCount} pending upload'),
    _ => const Text('Synced'),
  },
);

RecordSyncStateBuilder(
  engine: sync, entity: 'bills', localId: bill.id,
  builder: (context, state) => state == RecordSyncState.synced
      ? const SizedBox()
      : const Icon(Icons.cloud_off, size: 14),
);
```

`RecordSyncStateBuilder` only re-reads a record's state when an event
concerns that record or a run ends, so it is cheap on every row of a long
list. The same stream is available as `sync.watchRecordState(entity, id)`.

`SyncStatus.nextRetryAt` tells when operations that hit a temporary failure
(5xx, 429, timeout) will be retried — automatically, no trigger needed.

Failed operations stay in the outbox along with their error:

```dart
for (final op in await sync.failedOperations()) {
  print('${op.entity}/${op.localId}: ${op.lastError}');
}
await sync.retryFailed();          // all of them, or retryFailed(op.id)
await sync.discard(op.id);         // give up on one change

// Why has this bill not synced yet?
final ops = await sync.operationsOf('bills', bill.id);
final reason = ops.isEmpty ? null : ops.last.lastError;
```

Editing a record whose operation failed merges the fix into the queued
change and re-queues it with a fresh attempt budget — the usual case for a
validation error the user just corrected.

`sync.events` reports what happens (`operationRecorded`, `operationPushed`,
`operationFailed`, `operationRetried`, `operationDiscarded`, `recordPulled`,
`recordDeletedByServer`, `conflictResolved`, `runStarted`, `runFinished`,
`storeCleared`), for logging or refreshing a screen.

## Writing adapters

### Local adapter

```dart
class CustomersLocal extends LocalAdapter {
  CustomersLocal(this.db);
  final AppDatabase db;

  @override
  Future<String> applyRemote(Map<String, dynamic> record,
      {required String? localId, required String serverId}) async {
    // Reference fields in `record` already hold local ids.
    final row = CustomersCompanion(
      id: localId == null ? const Value.absent() : Value(int.parse(localId)),
      name: Value(record['name']),
      accountId: Value(record['accountId']),
      serverId: Value(int.parse(serverId)),
    );
    final id = await db.into(db.customers).insertOnConflictUpdate(row);
    return '${localId ?? id}';
  }

  @override
  Future<void> applyRemoteDelete(String localId, {required String serverId}) =>
      (db.delete(db.customers)..where((c) => c.id.equals(int.parse(localId)))).go();

  // Optional: keep your own server_id column up to date.
  @override
  Future<void> onServerIdAssigned(String localId, String serverId,
          {Map<String, dynamic>? serverRecord}) =>
      db.customStatement('UPDATE customers SET server_id = ? WHERE id = ?',
          [int.parse(serverId), int.parse(localId)]);

  // Optional: records created before flutter_offline_first_sync was installed.
  @override
  Future<String?> findServerId(String localId) async => (await db
          .customSelect('SELECT server_id FROM customers WHERE id = ?',
              variables: [Variable(int.parse(localId))])
          .getSingleOrNull())
      ?.data['server_id']
      ?.toString();
}
```

When one endpoint creates several records at once (e.g. account + profile +
customer), record that call under one entity with `recordCreate`, then
declare the others with `recordCreatedVia('customers', id, viaEntity: ...,
viaLocalId: ...)`. Records that reference them wait until that push
completes. Afterwards, register the returned ids via `registerMapping`
inside `onServerIdAssigned`.

Use `buildPushPayload` to send freshly read data (e.g. a bill with its
current line items) instead of the payload recorded with the operation.

### Remote adapter

`RestRemoteAdapter` maps create/update/delete to `POST`/`PUT`/`DELETE`, sends
an idempotency-key header, and understands `{"success": false, "message":
...}` responses that come back with HTTP 200. Responses are mapped as
follows:

| Response | Outcome |
|---|---|
| 2xx | success |
| 401 (`unauthorizedStatusCodes`) | unauthorized: sync stops until the next sign-in |
| `isAlreadyExists` returns true for a create | success: the existing record is adopted |
| 409 | conflict |
| 404 to a delete | success (already gone) |
| 408, 425, 429, 5xx | retry, honoring `Retry-After` (seconds or HTTP date) |
| anything else, including 3xx and 403 | rejected: the operation is kept as failed |

403 usually means "not allowed for this record", so it fails that one
operation instead of stopping all sync; use `unauthorizedStatusCodes: {401,
403}` if your server answers 403 to an expired token. Error messages are
read from `message`, RFC 7807 problem details (`title`, `detail`) and
validation `errors`; override `errorMessage` to customize them.

The transport is your choice:

```dart
Future<RestResponse> transport(RestRequest r) async {
  final uri = Uri.parse('$baseUrl/${r.path}')
      .replace(queryParameters: r.query.isEmpty ? null : r.query);
  final request = http.Request(r.method, uri)
    ..headers.addAll({'Content-Type': 'application/json', ...auth(), ...r.headers});
  if (r.body != null) request.body = jsonEncode(r.body);
  final response = await http.Response.fromStream(await client.send(request));
  return RestResponse(
    statusCode: response.statusCode,
    body: response.body.isEmpty ? null : jsonDecode(utf8.decode(response.bodyBytes)),
    headers: response.headers,
  );
}
```

Let transport exceptions (`SocketException`, `TimeoutException`,
`ClientException`, dio connection errors) propagate as-is. The engine treats
them as "offline" and does not count them as an attempt (see
`isNetworkError`, which also works in release web builds). **Do not** turn
them into fake HTTP responses.

For any unusual API, implement `RemoteAdapter` directly and return a
`PushOutcome`:

| Outcome | When | Effect |
|---|---|---|
| `success(serverId:)` | Change accepted (or already existed, with its id) | Operation is removed and the id is mapped |
| `networkError` | Server unreachable | Sync stops without counting an attempt |
| `retry` | 5xx, 429, or timeout | Backoff then retry; counts as an attempt |
| `rejected` | Validation error | Operation is marked failed; dependent records wait |
| `conflict` | Server holds a newer version | The conflict resolver decides |
| `unauthorized` | Token expired | Sync stops, `SyncPhase.authRequired` |

### SQL store

`SqlSyncStore` creates three tables (`sync_outbox`, `sync_id_map`,
`sync_meta`) using `IF NOT EXISTS`, so your schema needs no migration. Put it
in the same database as your data so a record and its operation are saved to
the outbox together.

Implement `SqlExecutor` on top of your database — see its doc comment for a
five-line `drift` example. For `sqflite`, route queries to the currently open
transaction through a `Zone`, so writes inside `SqlSyncStore.transaction`
land in the same transaction as your own:

```dart
class SqfliteExecutor implements SqlExecutor {
  SqfliteExecutor(this.db);
  final Database db;
  final _txn = Object(); // per instance: never joins another database's

  DatabaseExecutor get _current => Zone.current[_txn] as DatabaseExecutor? ?? db;

  @override
  Future<void> execute(String sql, [List<Object?> args = const []]) =>
      _current.execute(sql, args);

  @override
  Future<List<Map<String, Object?>>> query(String sql,
          [List<Object?> args = const []]) =>
      _current.rawQuery(sql, args);

  @override
  Future<T> transaction<T>(Future<T> Function() action) {
    if (Zone.current[_txn] != null) return action();
    return db.transaction((txn) => runZoned(action, zoneValues: {_txn: txn}));
  }
}
```

Run your own writes through the same executor's `transaction` (or
`sync.store.transaction`) so a row and its outbox entry commit together.

## Pulling

| `PullMode` | When to use it |
|---|---|
| `incremental` | The API supports filtering by modification time or returns a change cursor. Pair it with `UpdatedSincePagination` or `CursorPagination`; the cursor survives restarts. |
| `fullRefresh` | The API only supports full listing. Pair it with `PagePagination` or `OffsetPagination`. `LocalAdapter.onFullRefreshComplete` receives every server id seen, so you can delete rows the server no longer has. It is only called when every page arrived. |
| `none` | Push-only entities |

| Pagination | Request | Next page |
|---|---|---|
| `PagePagination` | `?page=1&count=500` | a full page means there is more, or `hasMoreOf` |
| `OffsetPagination` | `?offset=0&limit=500` | same as above |
| `UpdatedSincePagination` | `?updatedSince=<newest seen>&count=500` | a full page means there is more |
| `CursorPagination` | `?cursor=<from the response>&limit=500` | `nextCursor`/`next_cursor`/`nextPageToken` and `hasMore`/`has_more` in the body or in `meta`/`pagination`, or your own readers |

If the server caps the page size below `pullPageSize`, lower `pullPageSize`
or pass `hasMoreOf`, otherwise only the first page is pulled. Override
`PullPagination.toPageFromResponse` to read any other paging metadata. A
response in which no list of records can be found is reported as an error
rather than read as an empty page; pass `extractRecords` for unusual
formats.

Parent entities are pulled first, in an order inferred from `references` and
`dependsOn`. Registration order does not matter.

## Conflicts

A conflict is a pulled record whose local copy still has unpushed changes, or
a push that the server answered with `conflict`.

- `ConflictStrategy.keepLocal` (default): the device's change wins and is
  pushed with `force`.
- `ConflictStrategy.serverWins`: the server's copy is adopted and the local
  change is discarded.
- `ConflictStrategy.lastWriteWins`: compares `updatedAtOf(record)` against the
  local change's timestamp.
- `conflictResolver: (c) => ConflictResolution.merge({...})` for field-level
  merging.

## Configuration

```dart
SyncConfig(
  retryPolicy: RetryPolicy(maxAttempts: 8, baseDelay: Duration(seconds: 2)),
  debounce: Duration(milliseconds: 800),
  periodicInterval: Duration(minutes: 5),
  syncOnStart: true, syncOnResume: true, syncOnReconnect: true,
  autoPushAfterWrite: true,
  canSync: () => auth.isLoggedIn,
  onOperationFailed: (op, error) => showSnack('${op.entity}: $error'),
  logger: (level, message, [error, stack]) => debugPrint('[sync] $message'),
);
```

Per-entity settings on `SyncEntityConfig`: `pullPageSize`, `pushEnabled`,
`coalesce`, `mergePayload`, `serverIdOf`, `serverIdToJson`, `updatedAtOf`,
`isDeletedOf` (for server-deleted records), and `unknownReferencePolicy`.

`syncNow`, `pushNow` and `pullNow` accept `entities: {...}` to limit a run.
`clear()` (for logout) and `resetPullCursor()` wait for a run in progress.

## Testing

`InMemorySyncStore` behaves like a single-connection database, and
`ManualConnectivity` lets a test go offline and back. Turn the automatic
triggers off to drive every run yourself:

```dart
final sync = SyncEngine(
  store: InMemorySyncStore(),
  connectivity: ManualConnectivity(),
  config: const SyncConfig(
    syncOnStart: false, syncOnResume: false, syncOnReconnect: false,
    autoPushAfterWrite: false, periodicInterval: null,
  ),
  entities: [/* CallbackRemoteAdapter / CallbackLocalAdapter */],
);
await sync.recordCreate('notes', 'a', {'text': 'hi'});
final result = await sync.syncNow();
expect(result.pushed, 1);
```

To test against real SQLite, use `SqlSyncStore` with the executor above and
`sqflite_common_ffi`.

## Migrating an existing app

1. Register existing rows once:
   `await sync.registerMapping('customers', row.id, row.serverId)`, or
   implement `LocalAdapter.findServerId`/`findLocalId` to read a `server_id`
   column on demand.
2. Enqueue rows that were never pushed (`is_sync = 0`) via
   `recordCreate`/`recordUpdate`.
3. Route every create, update and delete in the app through the local
   database with `record*`, and remove the "if online call the API, else
   save locally" branches.
4. Replace a manual "sync everything" screen with `sync.syncNow()` and
   `SyncStatusBuilder`.

## Additional information

Issues and pull requests are welcome on
[GitHub](https://github.com/ahmedalgarbani/flutter_offline_sync).
