import '../core/outcomes.dart';

/// Talks to the server for one entity.
abstract class RemoteAdapter {
  const RemoteAdapter();

  /// Sends one change. Return a [PushOutcome] or throw; thrown errors go
  /// through `SyncConfig.classifyError`.
  Future<PushOutcome> push(PushRequest request);

  /// Fetches one page of server changes. Throw on failure (a
  /// [SyncNetworkException] stops the run quietly).
  Future<PullPage> pull(PullRequest request);
}

/// A [RemoteAdapter] built from functions.
class CallbackRemoteAdapter extends RemoteAdapter {
  const CallbackRemoteAdapter({
    required Future<PushOutcome> Function(PushRequest request) push,
    Future<PullPage> Function(PullRequest request)? pull,
  })  : _push = push,
        _pull = pull;

  final Future<PushOutcome> Function(PushRequest request) _push;
  final Future<PullPage> Function(PullRequest request)? _pull;

  @override
  Future<PushOutcome> push(PushRequest request) => _push(request);

  @override
  Future<PullPage> pull(PullRequest request) =>
      _pull?.call(request) ??
      Future.value(const PullPage(records: [], hasMore: false));
}
