RxJava Replaying Share
======================

`ReplayingShare` is an RxJava 3 transformer which combines `replay(1)`, `publish()`, and
`refCount()` operators.

Unlike traditional combinations of these operators, `ReplayingShare` caches the last emitted
value from the upstream observable or flowable *only* when one or more downstream subscribers are
connected. This allows expensive upstream sources to be shut down when no one is listening while
also replaying the last value seen by *any* subscriber to new ones.


|                                                                                  | replayingShare() | replay(1).refCount() | publish().refCount() | replay(1).autoConnect(1) |
|----------------------------------------------------------------------------------|------------------|----------------------|----------------------| -------------------------|
| Disconnects from upstream when there are no subscribers                          | ✅               | ✅                   | ✅                   | ❌                       |
| Replays the latest value to new subscribers when other subscribers are active    | ✅               | ✅                   | ❌                   | ✅                       |
| Replays the latest value to new subscribers when no other subscribers are active | ✅               | ❌                   | ❌                   | ✅                       |



![marble diagram](marbles.png)


Usage
-----

Apply with `compose` to an upstream `Observable` or `Flowable` and cache the resulting instance for
all new subscribers.

```java
@Singleton class Chart {
  private final Observable<Bitmap> chart;

  @Inject Chart(Observable<List<Data>> data) {
    chart = data.debounce(1, SECONDS)
        .map(list -> bigExpensiveRenderChartToBitmapFunction(list))
        .compose(ReplayingShare.instance());
  }

  Observable<Bitmap> data() {
    return chart;
  }
}
```

Kotlin users can use the operator via an extension function.

```kotlin
@Singleton class Chart
@Inject constructor(data: Observable<List<Data>>) {
  val chart: Observable<Bitmap> = data.debounce(1, SECONDS)
      .map(list -> bigExpensiveRenderChartToBitmapFunction(list))
      .replayingShare()
}
```

Note: This operator is designed for composition with infinite or extremely long-lived streams. Any
terminal event will clear the cached value.


Download
--------

Gradle:
```groovy
implementation 'com.jakewharton.rx3:replaying-share:3.0.0'
// Optional:
implementation 'com.jakewharton.rx3:replaying-share-kotlin:3.0.0'
```
Maven:
```xml
<dependency>
  <groupId>com.jakewharton.rx3</groupId>
  <artifactId>replaying-share</artifactId>
  <version>3.0.0</version>
</dependency>
<!-- Optional: -->
<dependency>
  <groupId>com.jakewharton.rx3</groupId>
  <artifactId>replaying-share-kotlin</artifactId>
  <version>3.0.0</version>
</dependency>
```

Snapshots of the development version are available in [Sonatype's `snapshots` repository][snap].


License
-------

    Copyright 2016 Jake Wharton

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.



 [snap]: https://oss.sonatype.org/content/repositories/snapshots/
