import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;

class ChannelAuthorization {
  final String auth;
  final String? channelData;
  const ChannelAuthorization(this.auth, {this.channelData});
}

typedef ChannelAuthorizer = Future<ChannelAuthorization> Function(
    String socketId, String channel);

/// Posts to your application's authenticated authorization route.
/// Keep the app secret on the backend. Close this authorizer when its owner ends.
class HttpChannelAuthorizer {
  final Uri endpoint;
  final FutureOr<Map<String, String>> Function()? headers;
  final Duration timeout;
  final http.Client _http;
  final bool _ownsClient;
  bool _closed = false;

  HttpChannelAuthorizer(this.endpoint,
      {this.headers,
      this.timeout = const Duration(seconds: 30),
      http.Client? client})
      : _http = client ?? http.Client(),
        _ownsClient = client == null {
    if (!['http', 'https'].contains(endpoint.scheme) ||
        endpoint.host.isEmpty ||
        endpoint.userInfo.isNotEmpty ||
        timeout <= Duration.zero) {
      throw ArgumentError(
          'A valid HTTP(S) authorization URL and positive timeout are required');
    }
  }

  Future<ChannelAuthorization> call(String socketId, String channel) async {
    if (_closed) throw StateError('Authorizer is closed');
    final customHeaders = await headers?.call() ?? <String, String>{};
    if (_closed) throw StateError('Authorizer is closed');
    final request = http.Request('POST', endpoint)
      ..followRedirects = false
      ..headers.addAll(customHeaders)
      ..headers['Content-Type'] = 'application/json'
      ..body = jsonEncode({'socket_id': socketId, 'channel_name': channel});
    final response = await _http
        .send(request)
        .then(http.Response.fromStream)
        .timeout(timeout);
    if (_closed) throw StateError('Authorizer is closed');
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw StateError(
          'Channel authorization failed (HTTP ${response.statusCode})');
    }
    final data = jsonDecode(response.body);
    if (data is! Map<String, dynamic> ||
        data['auth'] is! String ||
        (data['auth'] as String).trim().isEmpty ||
        (data['channel_data'] != null && data['channel_data'] is! String)) {
      throw const FormatException('Invalid channel authorization response');
    }
    return ChannelAuthorization(data['auth'] as String,
        channelData: data['channel_data'] as String?);
  }

  /// A supplied HTTP client remains caller-owned. Timeout stops waiting, but a
  /// supplied client may keep the network request alive until it is closed.
  void close() {
    if (_closed) return;
    _closed = true;
    if (_ownsClient) _http.close();
  }
}
