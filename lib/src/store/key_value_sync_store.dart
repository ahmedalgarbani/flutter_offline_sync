part of 'memory_sync_store.dart';

/// A place to keep one string under a key: SharedPreferences, Hive, Isar,
/// ObjectBox, GetStorage, flutter_secure_storage, a file, ...
///
/// Implement it, or build one from two functions with
/// [CallbackKeyValueStorage].
abstract class KeyValueStorage {
  const KeyValueStorage();

  /// The value stored under [key], or null.
  Future<String?> read(String key);

  /// Stores [value] under [key]; null removes the key. The engine relies on
  /// the write being complete (and durable) when the future completes.
  Future<void> write(String key, String? value);
}

/// A [KeyValueStorage] built from functions.
///
/// SharedPreferences:
/// ```dart
/// final prefs = await SharedPreferences.getInstance();
/// CallbackKeyValueStorage(
///   read: (key) async => prefs.getString(key),
///   write: (key, value) async => value == null
///       ? await prefs.remove(key)
///       : await prefs.setString(key, value),
/// )
/// ```
///
/// Hive:
/// ```dart
/// final box = await Hive.openBox<String>('sync');
/// CallbackKeyValueStorage(
///   read: (key) async => box.get(key),
///   write: (key, value) =>
///       value == null ? box.delete(key) : box.put(key, value),
/// )
/// ```
class CallbackKeyValueStorage extends KeyValueStorage {
  const CallbackKeyValueStorage({
    required Future<String?> Function(String key) read,
    required Future<void> Function(String key, String? value) write,
  })  : _read = read,
        _write = write;

  final Future<String?> Function(String key) _read;
  final Future<void> Function(String key, String? value) _write;

  @override
  Future<String?> read(String key) => _read(key);

  @override
  Future<void> write(String key, String? value) => _write(key, value);
}

/// A durable [SyncStore] for apps that keep their data in a key/value
/// store (Hive, SharedPreferences, Isar, ObjectBox, ...) rather than SQLite.
///
/// It works in memory like [InMemorySyncStore] and saves its whole state as
/// one JSON string to [storage] under [key] after every committed change,
/// before the change's future completes. It suits outboxes of up to a few
/// thousand operations; with SQLite available, prefer `SqlSyncStore`.
///
/// Your own writes cannot share a transaction with this store, so record
/// each change right after writing it locally:
///
/// ```dart
/// await box.put(note.id, note.toJson());
/// await sync.recordCreate('notes', note.id, note.toJson());
/// ```
class KeyValueSyncStore extends InMemorySyncStore {
  KeyValueSyncStore(this.storage, {this.key = 'flutter_offline_first_sync'});

  final KeyValueStorage storage;

  /// Key the state is saved under. Use one per account to keep separate
  /// outboxes.
  final String key;

  bool _loaded = false;

  @override
  bool get _persistent => true;

  @override
  Future<void> _persist() {
    // Saving before the saved state was loaded would overwrite it.
    if (!_loaded) {
      throw StateError('KeyValueSyncStore.init() must complete before the '
          'first change (SyncEngine calls it for you)');
    }
    return storage.write(key, jsonEncode(_snapshot()));
  }

  /// Loads the saved state. Throws a [FormatException] when the saved value
  /// cannot be read; it is left untouched, so no change is lost silently.
  @override
  Future<void> init() => _serialized(() async {
        if (_loaded) return;
        final saved = await storage.read(key);
        if (saved != null && saved.isNotEmpty) {
          try {
            _restore(Map<String, Object?>.from(jsonDecode(saved) as Map));
          } catch (e) {
            throw FormatException(
                'The sync state saved under "$key" cannot be read: $e');
          }
        }
        _loaded = true;
      });
}
