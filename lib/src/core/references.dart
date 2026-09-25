/// Declares that a field of an entity's payload points at another entity.
///
/// The engine uses references to:
/// * push a parent before its children (a child waits while the parent's
///   create is still in the outbox),
/// * replace local ids with server ids when pushing,
/// * replace server ids with local ids when pulling.
///
/// [path] is a dotted path into the JSON payload. A segment ending in `[]`
/// iterates a list:
///
/// ```dart
/// SyncReference('customerId', 'customers')
/// SyncReference('items[].itemId', 'items')
/// SyncReference('payment.accountId', 'accounts')
/// ```
class SyncReference {
  SyncReference(this.path, this.target) : _segments = _parse(path);

  final String path;

  /// Name of the referenced entity.
  final String target;

  final List<_Segment> _segments;

  /// All non-null values at [path] in [json].
  List<Object> read(Map<String, dynamic> json) {
    final out = <Object>[];
    _read(json, 0, out);
    return out;
  }

  /// Returns a deep copy of [json] with every value at [path] replaced by
  /// `map(value)`. Values for which [map] returns the same object are kept.
  Map<String, dynamic> rewrite(
    Map<String, dynamic> json,
    Object? Function(Object value) map,
  ) {
    final copy = deepCopyJson(json);
    _rewrite(copy, 0, map);
    return copy;
  }

  void _read(Object? node, int index, List<Object> out) {
    if (node is! Map) return;
    final segment = _segments[index];
    final value = node[segment.key];
    if (value == null) return;
    final last = index == _segments.length - 1;
    if (segment.isList) {
      if (value is! List) return;
      for (final element in value) {
        if (last) {
          if (element != null) out.add(element as Object);
        } else {
          _read(element, index + 1, out);
        }
      }
    } else if (last) {
      out.add(value as Object);
    } else {
      _read(value, index + 1, out);
    }
  }

  void _rewrite(Object? node, int index, Object? Function(Object) map) {
    if (node is! Map) return;
    final segment = _segments[index];
    final value = node[segment.key];
    if (value == null) return;
    final last = index == _segments.length - 1;
    if (segment.isList) {
      if (value is! List) return;
      for (var i = 0; i < value.length; i++) {
        final element = value[i];
        if (last) {
          if (element != null) value[i] = map(element as Object);
        } else {
          _rewrite(element, index + 1, map);
        }
      }
    } else if (last) {
      node[segment.key] = map(value as Object);
    } else {
      _rewrite(value, index + 1, map);
    }
  }

  static List<_Segment> _parse(String path) {
    if (path.trim().isEmpty) {
      throw ArgumentError.value(path, 'path', 'must not be empty');
    }
    return path.split('.').map((raw) {
      final isList = raw.endsWith('[]');
      final key = isList ? raw.substring(0, raw.length - 2) : raw;
      if (key.isEmpty) {
        throw ArgumentError.value(path, 'path', 'has an empty segment');
      }
      return _Segment(key, isList);
    }).toList(growable: false);
  }

  @override
  String toString() => 'SyncReference($path -> $target)';
}

class _Segment {
  const _Segment(this.key, this.isList);
  final String key;
  final bool isList;
}

/// Deep-copies JSON-like data (maps, lists, primitives).
Map<String, dynamic> deepCopyJson(Map json) {
  return json.map((key, value) => MapEntry(key.toString(), _copy(value)));
}

Object? _copy(Object? value) {
  if (value is Map) return deepCopyJson(value);
  if (value is List) return value.map(_copy).toList();
  return value;
}
