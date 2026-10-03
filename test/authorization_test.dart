import 'dart:convert';
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:pubb/pubb.dart';
import 'package:test/test.dart';

void main() {
  test('real HTTP auth uses current session headers and forwards channel_data',
      () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    var token = 'first';
    final requests = <Map<String, dynamic>>[];
    server.listen((request) async {
      requests.add({
        'body': jsonDecode(await utf8.decoder.bind(request).join()),
        'token': request.headers.value('Authorization')
      });
      request.response.headers.contentType = ContentType.json;
      request.response.write(
          jsonEncode({'auth': 'signed', 'channel_data': '{"user_id":"u1"}'}));
      await request.response.close();
    });
    final auth = HttpChannelAuthorizer(
        Uri.parse('http://127.0.0.1:${server.port}/auth'),
        headers: () => {'Authorization': 'Bearer $token'});
    try {
      expect((await auth.call('1.2', 'presence-room')).channelData,
          '{"user_id":"u1"}');
      token = 'second';
      await auth.call('2.3', 'presence-room');
      expect(requests.first['body'],
          {'socket_id': '1.2', 'channel_name': 'presence-room'});
      expect(
          requests.map((r) => r['token']), ['Bearer first', 'Bearer second']);
    } finally {
      auth.close();
      await server.close(force: true);
    }
  });

  test('auth rejects status and malformed data without echoing the body',
      () async {
    for (final response in [
      http.Response('secret response', 403),
      http.Response('{}', 200)
    ]) {
      final auth = HttpChannelAuthorizer(Uri.parse('https://example.com/auth'),
          client: MockClient((r) async {
        expect(r.followRedirects, isFalse);
        return response;
      }));
      try {
        await expectLater(
            auth.call('1.2', 'private-room'),
            throwsA(predicate<Object>(
                (e) => !e.toString().contains('secret response'))));
      } finally {
        auth.close();
      }
    }
  });

  test('closed authorizer rejects new calls', () async {
    final auth = HttpChannelAuthorizer(Uri.parse('https://example.com/auth'));
    auth.close();
    await expectLater(auth.call('1.2', 'private-room'), throwsStateError);
  });
}
