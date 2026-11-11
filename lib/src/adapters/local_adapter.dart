import '../core/operation.dart';

/// One pulled record to write locally (see [LocalAdapter.applyRemoteBatch]).
class RemoteChange {
  const RemoteChange({
    required this.record,
    required this.localId,
    required this.serverId,
  });

  /// The record in local form: references already hold local ids.
  final Map<String, dynamic> record;

  /// The known local id for [serverId], or null when the record is new on
  /// this device.
  final String? localId;

  final String serverId;
}

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

  /// Inserts or updates the pulled records of one page and returns their
  /// local ids, one per change and in the same order.
  ///
  /// The default calls [applyRemote] for each. Override it to write the
  /// page with one statement or one database batch: for a first download of
  /// hundreds of thousands of rows that is the difference between minutes
  /// and seconds.
  ///
  /// ```dart
  /// @override
  /// Future<List<String>> applyRemoteBatch(List<RemoteChange> changes) async {
  ///   final ids = [for (final c in changes) c.localId ?? newLocalId()];
  ///   final batch = executor.current.batch(); // sqflite
  ///   for (var i = 0; i < changes.length; i++) {
  ///     batch.insert('products', toRow(ids[i], changes[i]),
  ///         conflictAlgorithm: ConflictAlgorithm.replace);
  ///   }
  ///   await batch.commit(noResult: true);
  ///   return ids;
  /// }
  /// ```
  ///
  /// It runs inside the page's transaction, like [applyRemote].
  Future<List<String>> applyRemoteBatch(List<RemoteChange> changes) async => [
        for (final change in changes)
          await applyRemote(change.record,
              localId: change.localId, serverId: change.serverId),
      ];

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
  Future<Map<String, dynamic>?> buildPushPayload(
          SyncOperation operation) async =>
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
    Future<List<String>> Function(List<RemoteChange> changes)? applyRemoteBatch,
    Future<void> Function(String localId, {required String serverId})?
        applyRemoteDelete,
    Future<void> Function(String localId, String serverId,
            {Map<String, dynamic>? serverRecord})?
        onServerIdAssigned,
    Future<void> Function(SyncOperation operation, {String? serverId})?
        onPushed,
    Future<Map<String, dynamic>?> Function(SyncOperation operation)?
        buildPushPayload,
    Future<String?> Function(String localId)? findServerId,
    Future<String?> Function(String serverId)? findLocalId,
    Future<void> Function(Set<String> serverIds)? onFullRefreshComplete,
  })  : _applyRemote = applyRemote,
        _applyRemoteBatch = applyRemoteBatch,
        _applyRemoteDelete = applyRemoteDelete,
        _onServerIdAssigned = onServerIdAssigned,
        _onPushed = onPushed,
        _buildPushPayload = buildPushPayload,
        _findServerId = findServerId,
        _findLocalId = findLocalId,
        _onFullRefreshComplete = onFullRefreshComplete;

  final Future<String> Function(
    Map<String, dynamic> record, {
    required String? localId,
    required String serverId,
  }) _applyRemote;
  final Future<List<String>> Function(List<RemoteChange> changes)?
      _applyRemoteBatch;
  final Future<void> Function(String localId, {required String serverId})?
      _applyRemoteDelete;
  final Future<void> Function(String localId, String serverId,
      {Map<String, dynamic>? serverRecord})? _onServerIdAssigned;
  final Future<void> Function(SyncOperation operation, {String? serverId})?
      _onPushed;
  final Future<Map<String, dynamic>?> Function(SyncOperation operation)?
      _buildPushPayload;
  final Future<String?> Function(String localId)? _findServerId;
  final Future<String?> Function(String serverId)? _findLocalId;
  final Future<void> Function(Set<String> serverIds)? _onFullRefreshComplete;

  @override
  Future<String> applyRemote(
    Map<String, dynamic> record, {
    required String? localId,
    required String serverId,
  }) =>
      _applyRemote(record, localId: localId, serverId: serverId);

  @override
  Future<List<String>> applyRemoteBatch(List<RemoteChange> changes) =>
      _applyRemoteBatch?.call(changes) ?? super.applyRemoteBatch(changes);

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
  Future<void> onPushed(SyncOperation operation, {String? serverId}) =>
      _onPushed?.call(operation, serverId: serverId) ?? Future.value();

  @override
  Future<Map<String, dynamic>?> buildPushPayload(SyncOperation operation) =>
      _buildPushPayload?.call(operation) ?? Future.value();

  @override
  Future<String?> findServerId(String localId) =>
      _findServerId?.call(localId) ?? Future.value();

  @override
  Future<String?> findLocalId(String serverId) =>
      _findLocalId?.call(serverId) ?? Future.value();

  @override
  Future<void> onFullRefreshComplete(Set<String> serverIds) =>
      _onFullRefreshComplete?.call(serverIds) ?? Future.value();
}
