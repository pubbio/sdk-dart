import 'dart:async';
import 'dart:convert';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'authorization.dart';

enum PubbConnectionState {
  initialized,
  connecting,
  connected,
  disconnected,
  failed
}

class PubbEvent {
  final String name;
  final dynamic data;
  const PubbEvent(this.name, this.data);
}

class Channel {
  final String name;
  final _events = StreamController<PubbEvent>.broadcast();
  Map<String, dynamic> _members = {};
  bool _subscribed = false;
  Channel._(this.name);
  Stream<PubbEvent> get events => _events.stream;
  Stream<dynamic> on(String event) =>
      events.where((e) => e.name == _canonical(event)).map((e) => e.data);
  Map<String, dynamic> get members => Map.unmodifiable(_members);
  bool get subscribed => _subscribed;
  void _emit(String event, dynamic data) {
    if (!_events.isClosed) _events.add(PubbEvent(event, data));
  }

  void _reset() {
    _members = {};
    _subscribed = false;
  }
}

String _canonical(String event) {
  if (event == 'subscription_error') return 'pubb:subscription_error';
  if (event.startsWith('pusher_internal:')) {
    return event.replaceFirst('pusher_internal:', 'pubb:');
  }
  if (event.startsWith('pusher:')) {
    return event.replaceFirst('pusher:', 'pubb:');
  }
  return event;
}

/// Client for Flutter mobile, desktop, web and standalone Dart.
/// Own and dispose one instance per app/session, not per widget rebuild.
class Pubb {
  final ChannelAuthorizer? authorizer;
  final Duration connectionTimeout;
  final Uri _url;
  final WebSocketChannel Function(Uri) _connector;
  final List<Duration> _retryDelays;
  final Map<String, Channel> _channels = {};
  final Map<String, Object> _pending = {};
  final _states = StreamController<PubbConnectionState>.broadcast();
  final _errors = StreamController<Object>.broadcast();
  PubbConnectionState _state = PubbConnectionState.initialized;
  String? _socketId;
  WebSocketChannel? _socket;
  StreamSubscription<dynamic>? _subscription;
  Timer? _retry, _handshake;
  int _generation = 0, _attempts = 0;
  bool _stopped = true, _disposed = false;

  Pubb(
      {required String appKey,
      Uri? wsHost,
      this.authorizer,
      this.connectionTimeout = const Duration(seconds: 15),
      WebSocketChannel Function(Uri)? connector,
      List<Duration>? retryDelays})
      : _url = _socketUrl(appKey, wsHost ?? Uri.parse('wss://ws.pubb.io')),
        _connector = connector ?? WebSocketChannel.connect,
        _retryDelays = List.unmodifiable(retryDelays ??
            const [
              Duration(seconds: 2),
              Duration(seconds: 4),
              Duration(seconds: 8),
              Duration(seconds: 16),
              Duration(seconds: 30)
            ]) {
    if (connectionTimeout <= Duration.zero ||
        _retryDelays.any((delay) => delay < Duration.zero)) {
      throw ArgumentError(
          'Timeout must be positive and retry delays nonnegative');
    }
  }

  PubbConnectionState get state => _state;
  String? get socketId => _socketId;
  Stream<PubbConnectionState> get states => _states.stream;
  Stream<Object> get errors => _errors.stream;

  void connect() {
    _ensureOpen();
    if (_socket != null) return;
    _retry?.cancel();
    _retry = null;
    _stopped = false;
    _attempts = 0;
    _open();
  }

  Channel subscribe(String name) {
    _ensureOpen();
    if (name.trim().isEmpty || name.length > 200) {
      throw ArgumentError('Invalid channel name');
    }
    final existing = _channels[name];
    if (existing != null) return existing;
    final channel = Channel._(name);
    _channels[name] = channel;
    if (_state == PubbConnectionState.connected) {
      unawaited(_subscribe(channel, _generation));
    }
    return channel;
  }

  void unsubscribe(String name) {
    final channel = _channels.remove(name);
    if (channel == null) return;
    _pending.remove(name);
    channel._reset();
    if (_state == PubbConnectionState.connected) {
      _send({'action': 'unsubscribe', 'channel': name}, _generation);
    }
    unawaited(channel._events.close());
  }

  /// Stops retries and clears presence. Existing channels are kept for reconnect.
  void disconnect() {
    _stopped = true;
    _retry?.cancel();
    _retry = null;
    _reset();
    _attempts = 0;
    _setState(PubbConnectionState.disconnected);
  }

  /// Permanent cleanup. Also close a separately owned HttpChannelAuthorizer.
  void dispose() {
    if (_disposed) return;
    disconnect();
    _disposed = true;
    for (final channel in _channels.values) {
      unawaited(channel._events.close());
    }
    _channels.clear();
    unawaited(_states.close());
    unawaited(_errors.close());
  }

  void _open() {
    if (_stopped || _disposed || _socket != null) return;
    final generation = ++_generation;
    _setState(PubbConnectionState.connecting);
    try {
      final socket = _connector(_url);
      _socket = socket;
      _handshake = Timer(
          connectionTimeout,
          () => _failed(
              generation, TimeoutException('Pubb connection timed out')));
      _subscription = socket.stream.listen((raw) => _handle(raw, generation),
          onError: (Object error) => _failed(
              generation, StateError('Pubb WebSocket connection failed')),
          onDone: () => _failed(
              generation, StateError('Pubb WebSocket connection closed')));
      unawaited(socket.ready.catchError((Object error) {
        _failed(generation, StateError('Pubb WebSocket connection failed'));
      }));
    } catch (_) {
      _failed(generation, StateError('Pubb WebSocket connection failed'));
    }
  }

  void _failed(int generation, Object error) {
    if (_generation != generation || _stopped || _disposed) return;
    _reset();
    _setState(PubbConnectionState.disconnected);
    _report(error);
    if (_attempts >= _retryDelays.length) {
      _setState(PubbConnectionState.failed);
      return;
    }
    final next = _generation;
    _retry = Timer(_retryDelays[_attempts++], () {
      _retry = null;
      if (!_stopped && !_disposed && _generation == next) _open();
    });
  }

  void _reset() {
    _generation++;
    _handshake?.cancel();
    _handshake = null;
    final subscription = _subscription;
    _subscription = null;
    if (subscription != null) unawaited(subscription.cancel());
    final socket = _socket;
    _socket = null;
    if (socket != null) {
      unawaited(socket.sink.close().catchError((Object _) {}));
    }
    _socketId = null;
    _pending.clear();
    for (final channel in _channels.values) {
      channel._reset();
    }
  }

  Future<void> _subscribe(Channel channel, int generation) async {
    final socketId = _socketId;
    if (socketId == null ||
        _generation != generation ||
        channel._subscribed ||
        _pending.containsKey(channel.name)) {
      return;
    }
    final ticket = Object();
    _pending[channel.name] = ticket;
    try {
      ChannelAuthorization? auth;
      if (channel.name.startsWith('private-') ||
          channel.name.startsWith('presence-')) {
        if (authorizer == null) {
          throw StateError('An authorizer is required for this channel');
        }
        auth = await authorizer!(socketId, channel.name);
        if (auth.auth.trim().isEmpty ||
            (channel.name.startsWith('presence-') &&
                auth.channelData == null)) {
          throw const FormatException('Invalid channel authorization');
        }
      }
      if (_generation != generation ||
          !identical(_channels[channel.name], channel) ||
          !identical(_pending[channel.name], ticket)) {
        return;
      }
      _send({
        'action': 'subscribe',
        'channel': channel.name,
        if (auth != null) 'auth': auth.auth,
        if (auth?.channelData != null) 'channel_data': auth!.channelData
      }, generation);
    } catch (_) {
      if (_generation == generation &&
          identical(_channels[channel.name], channel) &&
          identical(_pending[channel.name], ticket)) {
        _pending.remove(channel.name);
        channel._emit(
            'pubb:subscription_error', 'Channel authorization failed');
        _report(StateError('Channel authorization failed'));
      }
    }
  }

  void _handle(dynamic raw, int generation) {
    if (_generation != generation || _disposed) return;
    try {
      final packet =
          jsonDecode(raw is String ? raw : utf8.decode(raw as List<int>))
              as Map<String, dynamic>;
      final event = packet['event'] is String
          ? _canonical(packet['event'] as String)
          : null;
      dynamic data = packet['data'];
      if (data is String) {
        try {
          data = jsonDecode(data);
        } on FormatException {/* Error text may be plain text. */}
      }
      if (event == 'pubb:connection_established') {
        if (data is! Map ||
            data['socket_id'] is! String ||
            (data['socket_id'] as String).isEmpty) {
          throw const FormatException();
        }
        if (_socketId != null) return;
        _socketId = data['socket_id'] as String;
        _attempts = 0;
        _handshake?.cancel();
        _handshake = null;
        _setState(PubbConnectionState.connected);
        for (final channel in _channels.values.toList()) {
          unawaited(_subscribe(channel, generation));
        }
        return;
      }
      if (event == 'pubb:ping') {
        _send({
          'event':
              packet['event'] == 'pusher:ping' ? 'pusher:pong' : 'pubb:pong',
          'data': {}
        }, generation);
        return;
      }
      final channel = _channels[packet['channel']];
      if (channel == null) {
        if (event == 'pubb:error') _report(StateError('Pubb server error'));
        return;
      }
      switch (event) {
        case 'pubb:subscription_succeeded':
          if (channel.name.startsWith('presence-')) {
            final presence = (data as Map)['presence'] as Map?;
            channel._members =
                Map<String, dynamic>.from(presence?['hash'] as Map? ?? {});
          }
          _pending.remove(channel.name);
          channel._subscribed = true;
          channel._emit(event!, data);
        case 'pubb:subscription_error':
          _pending.remove(channel.name);
          channel._reset();
          channel._emit(event!, data);
        case 'pubb:member_added':
        case 'pubb:member_removed':
          if (!channel.name.startsWith('presence-')) return;
          final userId = (data as Map)['user_id'] as String;
          if (event == 'pubb:member_added') {
            channel._members[userId] = data['user_info'] ?? {};
          } else {
            channel._members.remove(userId);
          }
          channel._emit(event!, data);
        case null:
          final payload = data as Map;
          channel._emit(payload['name'] as String, payload['data']);
        default:
          // Unknown protocol control frames are ignored for forward compatibility.
          break;
      }
    } catch (_) {
      _report(const FormatException('Invalid Pubb WebSocket message'));
    }
  }

  void _send(Map<String, dynamic> data, int generation) {
    if (_generation != generation || _socket == null) return;
    try {
      _socket!.sink.add(jsonEncode(data));
    } catch (_) {
      _failed(generation, StateError('Pubb WebSocket send failed'));
    }
  }

  void _report(Object error) {
    if (!_errors.isClosed) _errors.add(error);
  }

  void _setState(PubbConnectionState value) {
    if (_state == value) return;
    _state = value;
    if (!_states.isClosed) _states.add(value);
  }

  void _ensureOpen() {
    if (_disposed) throw StateError('Pubb client is disposed');
  }

  static Uri _socketUrl(String key, Uri host) {
    if (key.trim().isEmpty ||
        !['ws', 'wss'].contains(host.scheme) ||
        host.host.isEmpty ||
        host.userInfo.isNotEmpty) {
      throw ArgumentError(
          'A public app key and ws:// or wss:// host are required');
    }
    return Uri(
        scheme: host.scheme,
        host: host.host,
        port: host.hasPort ? host.port : null,
        path: '/socket',
        queryParameters: {'appKey': key, 'protocol': '2'});
  }
}
