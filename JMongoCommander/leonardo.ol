include "console.iol"
include "file.iol"
include "time.iol"
include "string_utils.iol"
include "protocols/http.iol"
include "/public/interface/MongoDBConnector.iol"


interface HTTPInterface{
	RequestResponse:
	default(undefined)(undefined),
	getDatabase(undefined)(undefined)
}
inputPort HttpInput {
Protocol: http {
	.keepAlive = 0; // Do not keep connections open
	.debug = 0;
	.debug.showContent = 0;
	.format -> format;
	.contentType -> mime;
  .charset ="ISO-8859-1";
	.default = "default";
	.compression = false
}
Location: "socket://localhost:8000"
Interfaces: HTTPInterface
}


execution { concurrent }

init
{
  DIRECTORY_PROJ = "./www/";
  DEBUG_MODE = true;
	DEBUG_DEFAULT_MODE = false
}

main
{
	[ default( request )( response ) {
		scope( s ) {
			install( FileNotFound => println@Console( "File not found: " + file.filename )() );
				if (DEBUG_DEFAULT_MODE){
      		valueToPrettyString@StringUtils(request)(s);
      		println@Console(s)()
				};
			file.filename = DIRECTORY_PROJ + request.operation;
			if (DEBUG_DEFAULT_MODE){
				valueToPrettyString@StringUtils(file)(s);
				println@Console("file to recover" + s)()
			};
			requestMime = request.operation;
			if (DEBUG_DEFAULT_MODE){
				valueToPrettyString@StringUtils(requestMime)(s);
				println@Console("requestMime" + s)()
			};
			getMimeType@File( file.filename )( mime );
			if (DEBUG_DEFAULT_MODE){
      		valueToPrettyString@StringUtils(mime)(s);
      		println@Console("file mime" + s)()
       };
			mime.regex = "/";
			split@StringUtils( mime )( s );
			if ( s.result[0] == "text" ) {
				file.format = "text";
				format = "html"
			} else {
				file.format = format = "binary"
			};

			readFile@File( file )( response )
		}
	} ] { nullProcess }
[getDatabase(request)(response){
	connectValue.host = "localhost";
	connectValue.dbname ="admin";
	connectValue.username = "myUserAdmin";
	connectValue.password = "abc123";
	connectValue.port = 27017;
	connectValue.jsonStringDebug = true;
	connectValue.timeZone = "Europe/Berlin";
	connectValue.logStreamDebug = true;
	valueToPrettyString@StringUtils(connectValue)(s);
	println@Console(s)();
	connect@MongoDB(connectValue)();
	listDB@MongDB()(response)
	}] { nullProcess }
