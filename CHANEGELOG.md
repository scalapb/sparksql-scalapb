## 1.0.2

* Add support for Spark 3.3.x, remove support for Spark 3.0.x

## 1.0.1
* Add capability to create custom spark representatino for protobuf messages (see `withMessageEncoder`).
* Using the above feature, we added `withSparkTimestamps` that represents `google.protobuf.Timestamp` as spark timestamp.

## 1.0.0

* We now cross-build for latest 3 major version of Spark.

## 0.10.5

### Breaking changes
* Protobuf maps are now represented as Spark in maps. Prior to this change
  maps were represented as a list of key-value structs. (#79)

