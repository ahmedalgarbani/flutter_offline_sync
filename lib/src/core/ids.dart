import 'dart:math';

/// Id helpers.
///
/// Client-generated UUIDs are the safest local ids: they never collide with
/// server ids, so a reference can never be mistaken for the wrong record.
class SyncIds {
  SyncIds._();

  static final Random _random = Random.secure();

  /// Returns a random RFC 4122 version 4 UUID.
  static String uuid() {
    final bytes = List<int>.generate(16, (_) => _random.nextInt(256));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    final hex = bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
    return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-'
        '${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
  }

  /// Normalizes an id value (int, String, ...) to the string form the engine
  /// stores. Returns null for "no id" values: null, empty, `0` and `'0'`.
  static String? normalize(Object? value) {
    if (value == null) return null;
    final s = value.toString().trim();
    if (s.isEmpty || s == '0' || s == 'null') return null;
    return s;
  }

  /// Converts a stored [serverId] back to a JSON value, keeping the type of
  /// [original] when possible (an int field stays an int).
  static Object toJsonValue(String serverId, Object? original) {
    if (original is num) {
      return int.tryParse(serverId) ?? serverId;
    }
    return serverId;
  }
}
