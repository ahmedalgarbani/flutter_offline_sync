import '../adapters/local_adapter.dart';
import '../adapters/remote_adapter.dart';
import 'conflict.dart';
import 'references.dart';

/// How an entity is pulled.
enum PullMode {
  /// Resume from the stored cursor; only changes since the last pull are
  /// fetched (needs a backend filter such as `updatedSince`).
  incremental,

  /// Always start from the first page and report every id seen to
  /// `LocalAdapter.onFullRefreshComplete`. For backends without deltas.
  fullRefresh,

  /// Push only.
  none,
}

/// What to do with a reference to a local id that is not known as a
/// locally created record and has no server id mapping.
enum UnknownReferencePolicy {
  /// Send the value unchanged (it is assumed to already be a server id,
  /// e.g. master data pulled before the engine was installed).
  passThrough,

  /// Fail the operation with a clear message.
  reject,
}

Object? _defaultServerIdOf(Map<String, dynamic> record) =>
    record['id'] ?? record['Id'];

Map<String, dynamic> _shallowMerge(
        Map<String, dynamic> previous, Map<String, dynamic> next) =>
    {...previous, ...next};

/// Describes one synchronized entity (table / resource).
class SyncEntityConfig {
  SyncEntityConfig({
    required this.name,
    required this.remote,
    required this.local,
    this.references = const [],
    this.dependsOn = const [],
    this.serverIdOf = _defaultServerIdOf,
    this.updatedAtOf,
    this.isDeletedOf,
    this.pullMode = PullMode.incremental,
    this.pullPageSize = 500,
    this.pushEnabled = true,
    this.coalesce = true,
    this.mergePayload = _shallowMerge,
    ConflictResolver? conflictResolver,
    ConflictStrategy conflictStrategy = ConflictStrategy.keepLocal,
    this.unknownReferencePolicy = UnknownReferencePolicy.passThrough,
  })  : assert(name.isNotEmpty),
        assert(pullPageSize > 0),
        conflictResolver = conflictResolver ??
            resolverFor(conflictStrategy, updatedAtOf: updatedAtOf);

  /// Unique name, used in the outbox and in [SyncReference.target].
  final String name;

  final RemoteAdapter remote;
  final LocalAdapter local;

  /// Fields that point at other entities.
  final List<SyncReference> references;

  /// Extra ordering dependencies not expressed by [references]: these
  /// entities are pulled before this one.
  final List<String> dependsOn;

  /// Reads the server id from a server record. Defaults to `id`.
  final Object? Function(Map<String, dynamic> record) serverIdOf;

  /// Reads the server's last-modified time (used by last-write-wins).
  final DateTime? Function(Map<String, dynamic> record)? updatedAtOf;

  /// Detects soft-deleted server records (tombstones) in a pulled page.
  final bool Function(Map<String, dynamic> record)? isDeletedOf;

  final PullMode pullMode;
  final int pullPageSize;

  /// Set to false for read-only entities.
  final bool pushEnabled;

  /// Merge consecutive unpushed changes of one record into one operation
  /// (create + update = create, update + update = update, create + delete =
  /// nothing).
  final bool coalesce;

  /// How two payloads of the same record are combined when coalescing.
  /// The default is a shallow merge (later keys win).
  final Map<String, dynamic> Function(
      Map<String, dynamic> previous, Map<String, dynamic> next) mergePayload;

  final ConflictResolver conflictResolver;

  final UnknownReferencePolicy unknownReferencePolicy;

  /// Every entity this one must follow.
  Set<String> get dependencies =>
      {...references.map((r) => r.target), ...dependsOn}..remove(name);
}
