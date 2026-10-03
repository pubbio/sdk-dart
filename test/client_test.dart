import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:pubb/pubb.dart';
import 'package:test/test.dart';

class TestServer {
  late HttpServer server;
  final sockets = <WebSocket>[];
  final frames = <Map<String, dynamic>>[];
  final paths = <Uri>[];
  bool handshake = true, rejectSockets = false;
  Future<void> start() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      paths.add(request.uri);
      final socket = await WebSocketTransformer.upgrade(request);
      sockets.add(socket);
      if (rejectSockets) {
        await socket.close();
        return;
      }
      socket.listen((raw) {
        final frame = jsonDecode(raw as String) as Map<String, dynamic>;
        frames.add(frame);
        if (frame['action'] == 'subscribe') {
          final channel = frame['channel'] as String;
          send(
              socket,
              'pusher_internal:subscription_succeeded',
              channel.startsWith('presence-')
                  ? {
                      'presence': {
                        'hash': {
                          'u1': {'name': 'Ada'}
                        }
                      }
                    }
                  : {},
              channel: channel);
        }
      });
      if (handshake) {
        send(socket, 'pubb:connection_established',
            {'socket_id': '${sockets.length}.1'});
      }
    });
  }

  Uri get url => Uri.parse('ws://127.0.0.1:${server.port}');
  void send(WebSocket socket, String event, dynamic data, {String? channel}) =>
      socket.add(jsonEncode({
        'event': event,
        'data': jsonEncode(data),
        if (channel != null) 'channel': channel
      }));
  Future<void> close() async {
    for (final socket in sockets) {
      await socket.close();
    }
    await server.close(force: true);
  }
}

Future<void> until(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 3));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('Condition did not become true');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

void main() {
  late TestServer server;
  late Pubb pubb;
  setUp(() async {
    server = TestServer();
    await server.start();
    pubb = Pubb(appKey: 'public +/key', wsHost: server.url);
  });
  tearDown(() async {
    pubb.dispose();
    await server.close();
  });

  test('real websocket subscription and application event', () async {
    final channel = pubb.subscribe('updates');
    final subscribed =
        channel.on('pusher_internal:subscription_succeeded').first;
    final message = channel.on('message').first;
    pubb.connect();
    await subscribed;
    expect(pubb.state, PubbConnectionState.connected);
    expect(pubb.socketId, '1.1');
    expect(server.paths.single.path, '/socket');
    expect(server.paths.single.queryParameters['appKey'], 'public +/key');
    expect(server.frames.single, {'action': 'subscribe', 'channel': 'updates'});
    server.sockets.single.add(jsonEncode({
      'channel': 'updates',
      'data': jsonEncode({
        'name': 'message',
        'data': {'text': 'Hello'}
      })
    }));
    expect(await message, {'text': 'Hello'});
    expect(channel.subscribed, isTrue);
  });

  test('presence state and aliases', () async {
    pubb.dispose();
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        authorizer: (socket, channel) async => const ChannelAuthorization(
            'signed',
            channelData: '{"user_id":"u1"}'));
    final channel = pubb.subscribe('presence-room');
    final ready = channel.on('pubb:subscription_succeeded').first;
    pubb.connect();
    await ready;
    expect(channel.members, {
      'u1': {'name': 'Ada'}
    });
    expect(server.frames.single['channel_data'], '{"user_id":"u1"}');
    final added = channel.on('pusher_internal:member_added').first;
    server.send(
        server.sockets.single,
        'pubb:member_added',
        {
          'user_id': 'u2',
          'user_info': {'name': 'Grace'}
        },
        channel: channel.name);
    await added;
    expect(channel.members.length, 2);
    final removed = channel.on('pubb:member_removed').first;
    server.send(server.sockets.single, 'pusher_internal:member_removed',
        {'user_id': 'u1'},
        channel: channel.name);
    await removed;
    expect(channel.members.keys, ['u2']);
    pubb.disconnect();
    expect(channel.members, isEmpty);
    expect(channel.subscribed, isFalse);
  });

  test('private channel without authorizer reports subscription error',
      () async {
    final channel = pubb.subscribe('private-room');
    final error = channel.on('pubb:subscription_error').first;
    pubb.connect();
    expect(await error, contains('authorization'));
    expect(server.frames.where((f) => f['action'] == 'subscribe'), isEmpty);
  });

  test('native subscription_error is emitted through the public alias',
      () async {
    final channel = pubb.subscribe('updates');
    final ready = channel.on('pubb:subscription_succeeded').first;
    pubb.connect();
    await ready;
    final error = channel.on('subscription_error').first;
    server.sockets.single.add(jsonEncode({
      'event': 'subscription_error',
      'channel': 'updates',
      'data': 'Access denied'
    }));
    expect(await error, 'Access denied');
    expect(channel.subscribed, isFalse);
  });

  test('stale authorization cannot subscribe after unsubscribe and resubscribe',
      () async {
    pubb.dispose();
    final oldAuth = Completer<ChannelAuthorization>();
    var calls = 0;
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        authorizer: (socket, channel) {
          calls++;
          return calls == 1
              ? oldAuth.future
              : Future.value(const ChannelAuthorization('new'));
        });
    pubb.subscribe('private-room');
    pubb.connect();
    await until(() => calls == 1);
    pubb.unsubscribe('private-room');
    final channel = pubb.subscribe('private-room');
    await channel.on('pubb:subscription_succeeded').first;
    oldAuth.complete(const ChannelAuthorization('stale'));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    final subscriptions =
        server.frames.where((f) => f['action'] == 'subscribe').toList();
    expect(subscriptions.length, 1);
    expect(subscriptions.single['auth'], 'new');
  });

  test('disconnect ignores outstanding authorization', () async {
    pubb.dispose();
    final auth = Completer<ChannelAuthorization>();
    var called = false;
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        authorizer: (_, __) {
          called = true;
          return auth.future;
        });
    pubb.subscribe('private-room');
    pubb.connect();
    await until(() => called);
    pubb.disconnect();
    auth.complete(const ChannelAuthorization('stale'));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(server.frames, isEmpty);
    expect(pubb.socketId, isNull);
  });

  test('reconnect reauthorizes with new socket id', () async {
    pubb.dispose();
    final ids = <String>[];
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        retryDelays: [Duration.zero],
        authorizer: (socket, channel) async {
          ids.add(socket);
          return ChannelAuthorization(socket);
        });
    final channel = pubb.subscribe('private-room');
    var subscriptions = 0;
    final listener = channel
        .on('pubb:subscription_succeeded')
        .listen((_) => subscriptions++);
    pubb.connect();
    await until(() => subscriptions == 1);
    await server.sockets.first.close();
    await until(() => subscriptions == 2);
    expect(ids, ['1.1', '2.1']);
    await listener.cancel();
  });

  test('retry attempts are bounded', () async {
    pubb.dispose();
    server.rejectSockets = true;
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        retryDelays: [Duration.zero, Duration.zero]);
    pubb.connect();
    await until(() => pubb.state == PubbConnectionState.failed);
    expect(server.sockets.length, 3);
  });

  test('handshake timeout and explicit disconnect cancel retries', () async {
    pubb.dispose();
    server.handshake = false;
    pubb = Pubb(
        appKey: 'key',
        wsHost: server.url,
        connectionTimeout: const Duration(milliseconds: 30),
        retryDelays: [const Duration(milliseconds: 100)]);
    final disconnected =
        pubb.states.firstWhere((s) => s == PubbConnectionState.disconnected);
    pubb.connect();
    await disconnected;
    pubb.disconnect();
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(server.sockets.length, 1);
    expect(pubb.state, PubbConnectionState.disconnected);
  });

  test('malformed frames report an error and keep the connection', () async {
    pubb.connect();
    await until(() => pubb.state == PubbConnectionState.connected);
    final error = pubb.errors.first;
    server.sockets.single.add('not json');
    expect(await error, isA<FormatException>());
    expect(pubb.state, PubbConnectionState.connected);
  });

  test('unsubscribe closes channel stream and dispose is final', () async {
    final channel = pubb.subscribe('updates');
    final ready = channel.on('pubb:subscription_succeeded').first;
    pubb.connect();
    await ready;
    final done = channel.events.drain<void>();
    pubb.unsubscribe('updates');
    await done;
    await until(() => server.frames.any((f) => f['action'] == 'unsubscribe'));
    pubb.dispose();
    pubb.dispose();
    expect(() => pubb.connect(), throwsStateError);
    expect(() => pubb.subscribe('room'), throwsStateError);
  });

  test('configuration validates before connecting', () {
    expect(() => Pubb(appKey: ''), throwsArgumentError);
    expect(() => Pubb(appKey: 'key', wsHost: Uri.parse('https://example.com')),
        throwsArgumentError);
    expect(() => Pubb(appKey: 'key', connectionTimeout: Duration.zero),
        throwsArgumentError);
  });
}
