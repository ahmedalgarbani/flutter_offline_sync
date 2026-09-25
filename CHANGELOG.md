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
