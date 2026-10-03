import 'package:pubb/pubb.dart';

void main() {
  // Pass your public app key via --define=PUBB_APP_KEY=... when compiling for web.
  const key = String.fromEnvironment('PUBB_APP_KEY');
  final pubb = Pubb(appKey: key);
  pubb.errors.listen((error) => print(error));
  final channel = pubb.subscribe('updates');
  channel.on('message').listen((data) => print(data));
  pubb.connect();
  // Call pubb.dispose() when the app/session ends.
}
