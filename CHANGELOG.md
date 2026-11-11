## 0.4.0

Scale release: the engine, the stores and the adapters now work in pages and
batches, so the cost of a record and the memory used no longer grow with the
amount of data. A first download is 3.5 times faster as it is and 20 times
faster with the new batch write; an upload is up to 44 times faster through
a bulk endpoint. One serious bug in automatic sync is fixed. Read
**Behavior changes** before upgrading.

Measured with the new [`benchmark/`](benchmark/sync_benchmark_test.dart) on
SQLite (a file database, the remote answering at once), 0.3.0 against 0.4.0:

| | 0.3.0 | 0.4.0 |
|---|---|---|
| Pull 100 000 records, `applyRemote` | 42.6 s | 12.0 s |
| Pull 100 000 records, `applyRemoteBatch` | | 2.1 s |
| Push 20 000 operations, one at a time | 98 s | 85 s |
| Push 20 000 operations, 8 side by side | | 14.1 s |
| Push 20 000 operations, bulk endpoint | | 2.2 s |
| Record 20 000 changes | 17.2 s | 10.5 s |
| A sync with nothing to do, 20 000 failed operations kept | 150 ms | 1.6 ms |

A million records were pulled in 22 s (`applyRemoteBatch`), at the same
rate per record as 100 000, and 200 000 operations were pushed through a
bulk endpoint in 27 s. Sending one operation at a time costs what it did:
it is bound by one request and two commits per operation, which is what
`pushBatchSize` is for.

### Behavior changes

- **Custom stores must extend `SyncStore`.** It has new members with working
  default implementations; a class that `implements SyncStore` no longer
  compiles until it says `extends` (or implements them).
- **New enum values.** `SyncPhase.serverUnavailable`,
  `SyncAbortReason.serverUnavailable` and `SyncAbortReason.cancelled`: a
  `switch` over these enums without a default needs the new cases.
- **A server that is unavailable pauses sync.** After three pushes in a row
  answered 429, 502, 503 or 504 (or `PushOutcome.retry(...,
  serverUnavailable: true)`), the run stops and the rest of the outbox is
  not sent. Sync resumes by itself after a delay that grows while the
  server stays down. Until now every operation in the outbox was sent, and
  lost an attempt, on every run. `SyncConfig(serverUnavailableThreshold:
  null)` restores that.
- **`dispose()` and `clear()` no longer wait for a long run to finish.** The
  run stops after the operations or the page it is working on and reports
  `SyncAbortReason.cancelled`.
- **Automatic syncs run in the zone `start()` was called in**, not in the
  zone of the call that triggered them (see the first fix).
- With more than 2 000 operations in the outbox, `status.pendingCount` and
  `failedCount` are refreshed at intervals (0.1 s at 20 000 operations)
  instead of after every change. They are exact whenever a run ends.
- An operation is held back by an earlier unpushed operation of its record,
  no longer by a failed later one.
- `SqlSyncStore.init()` creates one more index (`idx_<prefix>outbox_retry`).

### Fixes

- **A change recorded inside a database transaction was not pushed
  automatically.** `recordCreate` and the others scheduled the push from
  the zone they were called in. Called inside `executor.transaction(...)`,
  as the documentation recommends, that zone was the transaction: when the
  timer fired the transaction was over, and the push failed on its first
  statement (`transaction_closed` with sqflite). The change stayed in the
  outbox until something else triggered a sync.
- Costs that grew with the square of the outbox: the outbox was counted
  after every recorded and every pushed operation, and loaded whole for
  every push pass, every pulled page and at the end of every run.
- `InMemorySyncStore` copied its whole state at the start of every
  transaction and searched the id map row by row; `KeyValueSyncStore`
  inherits both fixes.
- Records waiting for a parent whose push had failed were claimed and put
  back, two writes each, on every sync.
- An incremental pull kept every server id it had seen in memory.
- A store error in the middle of a push left the operation "in flight", and
  unsendable, until the app was started again.

### Features

- `SyncEntityConfig.pushBatchSize` and `RemoteAdapter.pushBatch` (also on
  `CallbackRemoteAdapter`): send operations in one request to a bulk
  endpoint, or, with an adapter that has none, as that many requests side
  by side. Each operation still gets its own outcome.
- `LocalAdapter.applyRemoteBatch` and `RemoteChange` (also on
  `CallbackLocalAdapter`): write a pulled page with one statement. The
  default calls `applyRemote` for each record, as before.
- `SyncEngine.cancelSync()`.
- `SyncStatus.runPushed` and `runPulled`: the progress of a run.
- `SyncConfig.serverUnavailableThreshold`, `PushRetry.serverUnavailable`,
  `SyncPhase.serverUnavailable`.
- `SyncConfig.outboxPageSize`.
- `pendingOperations` and `failedOperations` take `limit` and `afterSeq`.
- `SyncStore`: `operationsPage`, `operationsForRecords`, `markOperations`,
  `deleteOperations`, `countOperations`, `nextAttemptAfter`, `clearBackoff`,
  `localIdsWithOperations`, `localIdsForServer`, `mappingsForLocal`,
  `putMappings`.
- `SyncReference.rewriteInPlace`.

### Other

- README: a "Large data sets" section.
- 322 package tests (from 202), including the store contract run against
  a store that implements only the required members.

## 0.3.0

Integration release: the engine now plugs into more of the storage, network
and state-management choices apps already make, and a review backed by
reproduction tests found and fixed eleven bugs. No API was removed or
renamed; read **Behavior changes** before upgrading.

### Behavior changes

- A `Retry-After` shorter than the retry policy's backoff no longer shortens
  it; a longer one is still honored. A `Retry-After` of zero, or an HTTP date
  in the past (a device clock ahead of the server's), used to resend the
  operation at once, within the same run, until every attempt was used up.
- `InMemorySyncStore` stores payloads as JSON, like `SqlSyncStore`: a payload
  holding a value JSON cannot represent (a `DateTime`, say) now fails when it
  is recorded, in tests as it already did in production.
- `recordDelete` of a record that never got a server id and whose create was
  discarded now sends nothing and returns null. It used to queue a delete
  that always failed.
- When the server deletes a record that has unpushed changes, an
  `operationDiscarded` event is sent for each dropped change.
- Writes can no longer postpone an automatic sync for more than
  `SyncConfig.maxDebounce` (5 seconds by default).
- `SyncRunResult.pulled` counts only records written locally, not those a
  local change won a conflict over.
- `SqlSyncStore` throws an `ArgumentError` for a `tablePrefix` holding
  anything but letters, digits and underscores.

### Fixes

- Writes arriving less than `debounce` apart (an autosaving form, a location
  tracker) postponed every automatic sync forever, including the periodic,
  reconnect and resume ones.
- `UpdatedSincePagination` ignored numeric timestamps (epoch seconds or
  milliseconds, row versions): the cursor was never saved, so every pull
  downloaded everything again. Numbers now compare as numbers, ISO dates as
  instants.
- A create that reached the server but whose response was lost (a timeout,
  a 502 after the save) was pulled back as a new record and stored twice on
  the device. Set the new `SyncEntityConfig.clientIdOf` when your server
  stores the client id.
- A push conflict resolved with `ConflictResolution.merge` was undone by an
  update of the same record queued behind it. Those updates are part of the
  conflict's `localPayload`, and are now folded into the merge.
- Deleting a record whose failed create had been discarded left a failed
  delete in the outbox (reproducible in the example app).
- `isNetworkError` missed dio 4 errors, socket errors dio 5 reports as
  `[unknown]`, and GraphQL `NetworkException`s, so time offline counted as
  failed attempts and could fail operations.
- `InMemorySyncStore` returned its own payload maps: changing an operation
  read from it changed the outbox.
- Server deletes dropped unpushed local changes without any event.
- An invalid `tablePrefix` produced broken SQL at runtime.

### Features

- `KeyValueSyncStore`, `KeyValueStorage` and `CallbackKeyValueStorage`: a
  durable outbox for apps on Hive, SharedPreferences, Isar, ObjectBox,
  GetStorage or any key/value store.
- `CallbackSqlExecutor`: an `SqlExecutor` from three functions for sqflite,
  SQLCipher, sqlite3, drift or floor, routing statements to the open
  transaction so your rows and the outbox commit together.
- `SyncEngine.watchStatus()`: the status as a stream, for Provider,
  Riverpod, Bloc, GetX or MobX.
- `SyncConfig.onUnauthorized`: refresh an expired session once per run and
  carry on.
- `syncNow(ignoreBackoff: true)` and `pushNow(ignoreBackoff: true)` for a
  "Sync now" button: operations waiting out a backoff are retried at once.
- `SyncConfig.maxDebounce`.
- `SyncEntityConfig.clientIdOf`.
- `SyncEngine.registerMappings` registers many existing rows in one
  transaction when migrating an app.
- `UpdatedSincePagination.compareCursors`.

### Other

- README: a "Works with your stack" section with storage, network, session,
  connectivity, state-management and background-sync recipes.
- 202 package tests (from 151), including the shared store contract run
  against `KeyValueSyncStore` and `CallbackSqlExecutor` on real SQLite.

## 0.2.0

Reliability release: many fixes found by a new test suite, plus a few
features. Read **Behavior changes** before upgrading.

### Behavior changes

- `RestRemoteAdapter`: 403 now fails the operation (`rejected`) instead of
  stopping all sync as `unauthorized`, so one forbidden record no longer
  halts every other push. Pass `unauthorizedStatusCodes: {401, 403}` to keep
  the old behavior.
- `RestRemoteAdapter`: 3xx responses are no longer treated as success (a
  redirect to a login page used to delete the change unsent).
- `RestRemoteAdapter.defaultExtractRecords` throws a `FormatException` when it
  finds no list of records, instead of returning an empty page (which made a
  full refresh delete every local row).
- `pullNow()` now waits for a run in progress and respects `canSync` and
  connectivity, like `syncNow()`.
- New `SyncEventType` values (`operationRetried`, `operationDiscarded`,
  `storeCleared`): exhaustive `switch`es need the new cases.
- `resetPullCursor('unknown')` throws `ArgumentError`, like other lookups.

### Fixes

- A `syncNow()` made while a push-only run was in progress returned that run
  and never pulled. Requests made during a run are now merged into one
  follow-up run, and the future completes once the requested work is done.
- Operations in backoff were only retried on the next write, reconnect,
  resume or periodic tick. The engine now retries them as soon as the
  backoff ends.
- An exception from `buildPushPayload`, a conflict resolver or a
  `LocalAdapter` callback left the operation in flight until the app
  restarted. It is now retried with backoff.
- If `onServerIdAssigned` or `onPushed` threw after the server accepted a
  create, the create was sent again after a restart. The id mapping is now
  always kept, and the error is reported in the run result.
- A full refresh that stopped early still called `onFullRefreshComplete`
  with a partial id set, so apps deleted rows that still exist on the server.
- A store that failed to open stayed failed forever and raised an unhandled
  async error. `start()` can now be retried, and runs report the error.
- `dispose()` during `start()` leaked the connectivity subscription, timers
  and lifecycle observer, then crashed on the disposed status notifier.
- `clear()` during a run could leave the id mapping of an in-flight create
  behind (for example after logout). `clear()` and `resetPullCursor()` now
  wait for the run.
- Editing a record whose failed operation had another change queued behind
  it left the record failed. All unsent changes of the record are now merged
  and queued again.
- `ConflictStrategy.keepLocal` on pull now marks the local change with
  `force`, as documented.
- `SyncRunResult.waiting` missed operations put in backoff during the run.
- Automatic runs pulled even with `SyncConfig.pullOnSync: false`.
- A throwing `onOperationFailed`, `logger` or `classifyError` could break a
  run. `onOperationFailed` now receives the operation in its failed state.
- Network errors were recognized by type name only, which fails in release
  web builds (minified names) and, on every platform, for the socket error
  subclass that `package:http` throws. Messages are now checked too, and dio
  connection and timeout errors are recognized.
- `InMemorySyncStore`: transactions could interleave, so a failing one could
  roll back another's committed writes, and stored payloads were shared with
  the caller. It now behaves like a single-connection database.
- Nested transactions are matched per store instance (`InMemorySyncStore`,
  `SqlSyncStore`); `SqlSyncStore.clear()` is atomic.
- `StreamConnectivity` no longer raises an unhandled error when its source
  stream fails.
- Payloads passed to `recordCreate`/`recordUpdate` are deep-copied, so later
  changes by the caller do not alter what is sent.
- `pendingOperations()` and `failedOperations()` open the store first.
- An engine created in a test's `setUp` no longer hangs in `testWidgets`.
- `RestRemoteAdapter`: `isAlreadyExists` is checked before a 409 becomes a
  conflict, so a 409 "already exists" can be adopted; `Retry-After` accepts
  HTTP dates; headers are matched case-insensitively.
- Example app: deleting a synced note threw and never synced.

### Features

- `SyncStatus.nextRetryAt`. `SyncStatus` has value equality, so listeners
  are only notified of real changes.
- `SyncEngine.operationsOf(entity, localId)` to show why a record has not
  synced.
- `SyncEngine.watchRecordState(entity, localId)`. `RecordSyncStateBuilder`
  uses it and no longer re-queries on every status change.
- `SyncEntityConfig.serverIdToJson`, e.g. `int.parse` to send numeric server
  ids when local ids are UUID strings.
- Pagination: `OffsetPagination`, `CursorPagination`, `hasMoreOf` for
  `PagePagination` and `OffsetPagination`, and
  `PullPagination.toPageFromResponse` for custom paging metadata.
- `RestRemoteAdapter`: `unauthorizedStatusCodes`, `errorMessage` (reads RFC
  7807 problem details and validation `errors`), `parseRetryAfter`,
  `RestResponse.header()`; records are also found under `records`, `rows`,
  `value` and one level down (`data.items`); a bare numeric body is read as
  the new id.
- `pushNow(entities:)`; `isNetworkError()` for custom `classifyError`
  functions; `ManualConnectivity.dispose()`; `CallbackLocalAdapter` accepts
  `onPushed` and `onFullRefreshComplete`.

### Other

- About 150 tests covering the engine, the REST adapter, both stores
  (including real SQLite through `sqflite_common_ffi`), the widgets and the
  example app.
- The example app now shows pulls, offline queueing and failure handling.
- Logs go to `dart:developer` under the name `flutter_offline_first_sync`.
- Stricter analysis options and a CI workflow.

## 0.1.1

- Update dependencies.

## 0.1.0

First version.

- `SyncEngine` with a durable outbox, dependency-aware push, local↔server id
  mapping and reference rewriting (including nested list paths).
- Resumable, parents-first pull with conflict resolution
  (`keepLocal`, `serverWins`, `lastWriteWins`, custom merge).
- Coalescing of unpushed changes; editing a failed record re-queues it.
- Retry with exponential backoff; network errors never count as attempts;
  failed operations are kept until retried or discarded.
- Idempotency keys and "already exists → adopt" handling.
- `InMemorySyncStore` and `SqlSyncStore` (drift, sqflite, any SQLite).
- `RestRemoteAdapter` with page and `updatedSince` pagination.
- Automatic triggers: after writes, on reconnect, on resume, periodic.
- `SyncStatusBuilder` and `RecordSyncStateBuilder` widgets.
