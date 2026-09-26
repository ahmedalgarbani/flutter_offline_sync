/// Offline-first sync engine for Flutter.
///
/// Start with [SyncEngine], describe each synchronized entity with
/// [SyncEntityConfig], and plug in your backend ([RemoteAdapter] or
/// [RestRemoteAdapter]) and your database ([LocalAdapter], [SqlSyncStore]).
library;

export 'src/adapters/local_adapter.dart';
export 'src/adapters/remote_adapter.dart';
export 'src/adapters/rest_remote_adapter.dart';
export 'src/connectivity.dart';
export 'src/core/conflict.dart';
export 'src/core/entity_config.dart';
export 'src/core/ids.dart';
export 'src/core/operation.dart';
export 'src/core/outcomes.dart';
export 'src/core/references.dart' show SyncReference, deepCopyJson;
export 'src/core/retry.dart';
export 'src/core/status.dart';
export 'src/engine/sync_config.dart';
export 'src/engine/sync_engine.dart';
export 'src/store/memory_sync_store.dart';
export 'src/store/sql_sync_store.dart';
export 'src/store/sync_store.dart';
export 'src/widgets/sync_status_builder.dart';
