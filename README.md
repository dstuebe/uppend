Uppend: an append-only, key-multivalue store 
============================================
[![Build Status](https://travis-ci.org/upserve/uppend.svg?branch=master)](https://travis-ci.org/upserve/uppend)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.upserve/uppend/badge.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.upserve%22%20AND%20a%3Auppend)

Uppend is an append-only, key-multivalue store, suitable for creating analytics
views from event streams.

Benefits:

* Simple configuration of event schema and processing
* Fast and compact serialization using [Colfer](https://github.com/pascaldekloe/colfer)
* Optimized to be I/O constrained on modest hardware
 
Tradeoffs:

* Individual values are immutable
* No ability list or range-scan keys
* Assumes a single writer process


Use
---

Maven:

```xml
<dependency>
    <groupId>com.upserve</groupId>
    <artifactId>uppend</artifactId>
    <version>1.0.0</version>
</dependency>
```

Gradle:
```gradle
compile 'com.upserve:uppend:1.0.0'
```

Hello world:

```java
//import com.upserve.uppend.*
DB db = DBMaker.memoryDB().make();
ConcurrentMap map = db.hashMap("map").make();
map.put("something", "here");
```

Development
-----------

To build Uppend, run:
 
```sh
./gradlew build
```