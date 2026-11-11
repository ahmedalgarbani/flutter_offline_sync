import '../core/outcomes.dart';

/// Talks to the server for one entity.
abstract class RemoteAdapter {
  const RemoteAdapter();

  /// Sends one change. Return a [PushOutcome] or throw; thrown errors go
  /// through `SyncConfig.classifyError`.
  Future<PushOutcome> push(PushRequest request);

  /// Sends several changes of this entity in one request, for servers with
  /// a bulk endpoint. Called instead of [push] when
  /// `SyncEntityConfig.pushBatchSize` is greater than 1.
  ///
  /// Return one [PushOutcome] per request, in the same order: each
  /// operation is settled on its own, so some can succeed while others are
  /// rejected, conflict or are retried. Throwing applies the classified
  /// error to all of them.
  ///
  /// The default returns null, which means "no bulk endpoint": the engine
  /// then sends the requests as separate [push] calls, side by side.
  Future<List<PushOutcome>?> pushBatch(List<PushRequest> requests) async =>
      null;

  /// Fetches one page of server changes. Throw on failure (a
  /// [SyncNetworkException] stops the run quietly).
  Future<PullPage> pull(PullRequest request);
}

/// A [RemoteAdapter] built from functions.
class CallbackRemoteAdapter extends RemoteAdapter {
  const CallbackRemoteAdapter({
    required Future<PushOutcome> Function(PushRequest request) push,
    Future<PullPage> Function(PullRequest request)? pull,
    Future<List<PushOutcome>> Function(List<PushRequest> requests)? pushBatch,
  })  : _push = push,
        _pull = pull,
        _pushBatch = pushBatch;

  final Future<PushOutcome> Function(PushRequest request) _push;
  final Future<PullPage> Function(PullRequest request)? _pull;
  final Future<List<PushOutcome>> Function(List<PushRequest> requests)?
      _pushBatch;

  @override
  Future<PushOutcome> push(PushRequest request) => _push(request);

  @override
  Future<List<PushOutcome>?> pushBatch(List<PushRequest> requests) =>
      _pushBatch?.call(requests) ?? Future.value();

  @override
  Future<PullPage> pull(PullRequest request) =>
      _pull?.call(request) ??
      Future.value(const PullPage(records: [], hasMore: false));
}
