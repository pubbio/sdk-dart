# Pubb Flutter / Dart client

Realtime subscriptions for Flutter mobile, desktop, web, and Dart 3.4+.
Supports public, private, and presence channels. Only a public app key belongs in
your app; keep the app secret and publishing on your backend.

For Android release builds, declare `android.permission.INTERNET` in your app's
AndroidManifest.xml. The default `wss://` connection uses the platform TLS trust store.

## Install

Until the package is published on pub.dev, add this to your pubspec.yaml:

```yaml
dependencies:
  pubb:
    git:
      url: https://github.com/pubbio/sdk-dart.git
      ref: main
```

Run `flutter pub get` or `dart pub get`. Commit pubspec.lock in your application
to pin the resolved revision.

## Subscribe

```dart
import 'package:pubb/pubb.dart';

final pubb = Pubb(appKey: 'YOUR_PUBLIC_APP_KEY');
pubb.states.listen((state) => print(state));
pubb.errors.listen((error) => print(error));
final channel = pubb.subscribe('updates');
final listener = channel.on('message').listen((data) => print(data));
channel.on('pubb:subscription_succeeded').listen((_) => print('Subscribed'));
pubb.connect();
```

`connect()` starts asynchronously; use `states` for connection changes and `state`
for its current value. `socketId` becomes available after the server handshake.
Own the instance outside a widget's build method. Cancel listeners and call
`pubb.dispose()` from the owning Flutter State's `dispose()` method.

`unsubscribe(name)` removes a channel and closes its streams. `disconnect()`
stops retries and clears presence but retains channels for a later `connect()`.
`dispose()` permanently closes the client. Automatic reconnect makes up to five
attempts with delays of 2, 4, 8, 16 and 30 seconds, then enters `failed`.
Connections time out after 15 seconds without a handshake. Subscriptions are
restored and reauthorized after reconnect. Streams do not replay events: refetch
durable application state after reconnect. Cancel unused listeners rather than
pausing them indefinitely, since Dart stream subscriptions buffer while paused.

## Private / presence channels

```dart
final auth = HttpChannelAuthorizer(
  Uri.parse('https://your-backend.example/pubb/auth'),
  headers: () async => {'Authorization': 'Bearer YOUR_SESSION_TOKEN'},
);
final pubb = Pubb(appKey: 'YOUR_PUBLIC_APP_KEY', authorizer: auth.call);
final room = pubb.subscribe('presence-room');
room.on('pubb:subscription_succeeded').listen((_) => print(room.members));
room.on('pubb:member_added').listen((_) => print(room.members));
room.on('pubb:member_removed').listen((_) => print(room.members));
pubb.connect();
// When the owning session ends: pubb.dispose(); auth.close();
```

Your authenticated backend receives JSON `socket_id` and `channel_name`, checks
the user's channel permission, and returns `auth` plus `channel_data` for presence.
Generate this response with a Pubb server SDK. The header callback runs for each
request, allowing refreshed session tokens. On web, configure your backend CORS;
cookie credentials can be enabled with a caller-owned HTTP BrowserClient.
A supplied HTTP client remains caller-owned. Timeouts stop waiting for auth;
an outstanding request may continue until its HTTP client is closed. Stale auth
responses are ignored after unsubscribe, disconnect, or a new connection.
You can supply your own `ChannelAuthorizer` callback instead.

This client uses Pubb's `/socket?appKey=...` protocol. Incoming `pusher:*` and
`pusher_internal:*` control names are accepted and emitted as `pubb:*`; legacy
names passed to `channel.on()` are aliases. This does not claim full compatibility
with an unmodified Pusher server or client.

## Development

```sh
dart pub get
dart analyze --fatal-infos
dart test
dart compile js example/main.dart -o /tmp/pubb-example.js
```

Tests cover the real local WebSocket/HTTP protocol and connection lifecycle. MIT.

## Subscription protocol compatibility

This client requests `protocol=2` when opening its WebSocket connection and handles
`pubb:subscription_succeeded`. It still accepts the legacy acknowledgement from
older servers. Current pubb servers send one acknowledgement per subscription:
existing clients without `protocol=2` keep their legacy wire event.
