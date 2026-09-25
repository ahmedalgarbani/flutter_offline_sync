import '../core/operation.dart';

/// Writes server data into the app's own tables for one entity.
///
/// The engine never owns your data tables; it only keeps its own metadata
/// (outbox, id map, cursors). Everything it passes here is in local form:
/// references already hold local ids.
abstract class LocalAdapter {
  const LocalAdapter();

  /// Inserts or updates a pulled record and returns its local id.
  ///
  /// [localId] is the known local id for [serverId], or null when the
  /// record is new on this device (insert it and return the new id).
  Future<String> applyRemote(
    Map<String, dynamic> record, {
    required String? localId,
    required String serverId,
  });

  /// The server deleted the record.
  Future<void> applyRemoteDelete(String localId, {required String serverId});

  /// A create was accepted. Store [serverId] if your table has a column for
  /// it; the engine's id map already has it either way. [serverRecord] is
  /// the server's copy when the response included one (server form).
  Future<void> onServerIdAssigned(
    String localId,
    String serverId, {
    Map<String, dynamic>? serverRecord,
  }) async {}

  /// Any operation was accepted by the server.
  Future<void> onPushed(SyncOperation operation, {String? serverId}) async {}

  /// Returns the payload to send, read fresh from the database (for example
  /// a bill with its current lines). Return null to send the payload that
  /// was recorded with the operation.
  Future<Map<String, dynamic>?> buildPushPayload(SyncOperation operation) async =>
      null;

  /// Fallback for records that existed before the engine was installed:
  /// the server id kept in your own `server_id` column.
  Future<String?> findServerId(String localId) async => null;

  /// Reverse of [findServerId].
  Future<String?> findLocalId(String serverId) async => null;

  /// Called after a full-refresh pull finished, with every server id the
  /// server returned. Use it to remove local rows the server no longer has.
  Future<void> onFullRefreshComplete(Set<String> serverIds) async {}
}

/// A [LocalAdapter] built from functions.
class CallbackLocalAdapter extends LocalAdapter {
  const CallbackLocalAdapter({
    required Future<String> Function(
      Map<String, dynamic> record, {
      required String? localId,
      required String serverId,
    }) applyRemote,
    Future<void> Function(String localId, {required String serverId})?
        applyRemoteDelete,
    Future<void> Function(String localId, String serverId,
            {Map<String, dynamic>? serverRecord})?
        onServerIdAssigned,
    Future<Map<String, dynamic>?> Function(SyncOperation operation)?
        buildPushPayload,
    Future<String?> Function(String localId)? findServerId,
    Future<String?> Function(String serverId)? findLocalId,
  })  : _applyRemote = applyRemote,
        _applyRemoteDelete = applyRemoteDelete,
        _onServerIdAssigned = onServerIdAssigned,
        _buildPushPayload = buildPushPayload,
        _findServerId = findServerId,
        _findLocalId = findLocalId;

  final Future<String> Function(
    Map<String, dynamic> record, {
    required String? localId,
    required String serverId,
  }) _applyRemote;
  final Future<void> Function(String localId, {required String serverId})?
      _applyRemoteDelete;
  final Future<void> Function(String localId, String serverId,
      {Map<String, dynamic>? serverRecord})? _onServerIdAssigned;
  final Future<Map<String, dynamic>?> Function(SyncOperation operation)?
      _buildPushPayload;
  final Future<String?> Function(String localId)? _findServerId;
  final Future<String?> Function(String serverId)? _findLocalId;

  @override
  Future<String> applyRemote(
    Map<String, dynamic> record, {
    required String? localId,
    required String serverId,
  }) =>
      _applyRemote(record, localId: localId, serverId: serverId);

  @override
  Future<void> applyRemoteDelete(String localId, {required String serverId}) =>
      _applyRemoteDelete?.call(localId, serverId: serverId) ?? Future.value();

  @override
  Future<void> onServerIdAssigned(
    String localId,
    String serverId, {
    Map<String, dynamic>? serverRecord,
  }) =>
      _onServerIdAssigned?.call(localId, serverId,
          serverRecord: serverRecord) ??
      Future.value();

  @override
  Future<Map<String, dynamic>?> buildPushPayload(SyncOperation operation) =>
      _buildPushPayload?.call(operation) ?? Future.value();

  @override
  Future<String?> findServerId(String localId) =>
      _findServerId?.call(localId) ?? Future.value();

  @override
  Future<String?> findLocalId(String serverId) =>
      _findLocalId?.call(serverId) ?? Future.value();
}
