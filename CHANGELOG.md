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
