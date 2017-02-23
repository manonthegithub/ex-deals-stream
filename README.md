# ex-deals-stream
Streaming proxy aggregator example

Reads events about stock deals from server via tcp, counts candlesticks and broadcasts them to clients.

Project was built and run with **java 8** and **sbt 0.13.x**, proper working with other versions of java and sbt is not guaranteed.

To build execute:
```
sbt clean assembly
```

To run find in folder **{projectPath}/target/scala-2.12/** assembled **jar** file
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

## To test:

1. Run sample streaming server than can be found in **{projectPath}/src/test/**:
  ```
  python sample-streaming-server.py
  ```
  Server accepts connections on port 15555 and sends sample data
  
2. Run application as described above

3. Connect to port 15556 and see coming data. To listen to port you can use nc:
  ```
  nc localhost 15556
  ```
  Multiple connections are supported, data is broadcast to each of them.
