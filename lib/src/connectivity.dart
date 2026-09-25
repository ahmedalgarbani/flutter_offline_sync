import 'dart:async';

/// Tells the engine when the device is (probably) online.
///
/// This is only a hint for scheduling: the real test is whether requests
/// succeed, and a network error always puts the engine in the offline state.
/// Wrap `connectivity_plus` or `internet_connection_checker_plus` with
/// [StreamConnectivity]:
///
/// ```dart
/// StreamConnectivity(
///   initial: await InternetConnection().hasInternetAccess,
///   changes: InternetConnection().onStatusChange
///       .map((s) => s == InternetStatus.connected),
/// )
/// ```
abstract class ConnectivitySource {
  bool get isOnline;
  Stream<bool> get onChanged;
}

/// Assumes the device is always online (network errors still pause sync).
class AlwaysOnline implements ConnectivitySource {
  const AlwaysOnline();
  @override
  bool get isOnline => true;
  @override
  Stream<bool> get onChanged => const Stream.empty();
}

/// Connectivity driven by a stream of online/offline values.
class StreamConnectivity implements ConnectivitySource {
  StreamConnectivity({required bool initial, required Stream<bool> changes})
      : _isOnline = initial {
    _subscription = changes.listen((value) {
      if (value == _isOnline) return;
      _isOnline = value;
      _controller.add(value);
    });
  }

  bool _isOnline;
  late final StreamSubscription<bool> _subscription;
  final _controller = StreamController<bool>.broadcast();

  @override
  bool get isOnline => _isOnline;

  @override
  Stream<bool> get onChanged => _controller.stream;

  Future<void> dispose() async {
    await _subscription.cancel();
    await _controller.close();
  }
}

/// Connectivity set by hand; handy in tests.
class ManualConnectivity implements ConnectivitySource {
  ManualConnectivity([this._isOnline = true]);

  bool _isOnline;
  final _controller = StreamController<bool>.broadcast();

  @override
  bool get isOnline => _isOnline;

  @override
  Stream<bool> get onChanged => _controller.stream;

  set online(bool value) {
    if (value == _isOnline) return;
    _isOnline = value;
    _controller.add(value);
  }
}
