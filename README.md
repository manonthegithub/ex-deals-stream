# ex-deals-stream
Streaming proxy aggregator example

Reads events about stock deals from server via tcp and broadcasts aggregated candlesticks events to clients.

To build execute:
```
sbt clean assembly
```

To run find in **{projectPath}/target/** assembled file
and execute:
```
java -jar ex-deals-stream-assembly-1.0.jar
```

Default host with events server: **localhost**

Default port for events server connection: **15555**

Default bind port: **15556**

To reconfigure defaults put **application.conf** file in the same directory with jar: 
```
eds {
  remote.port = 15555
  remote.host = localhost
  bind.port = 15556
}
```
