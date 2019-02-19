/*
 * Copyright 2016 Jake Wharton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jakewharton.rx;

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.PublishSubject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ReplayingShareObservableTest {
  @Test public void noInitialValue() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    TestObserver<String> observer = new TestObserver<>();
    observable.subscribe(observer);
    observer.assertNoValues();
  }

  @Test public void initialValueToNewSubscriber() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    observable.subscribe(observer1);
    observer1.assertNoValues();

    subject.onNext("Foo");
    observer1.assertValues("Foo");

    TestObserver<String> observer2 = new TestObserver<>();
    observable.subscribe(observer2);
    observer2.assertValues("Foo");
  }

  @Test public void initialValueToNewSubscriberAfterUnsubscribe() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    observable.subscribe(observer1);
    observer1.assertNoValues();

    subject.onNext("Foo");
    observer1.assertValues("Foo");
    observer1.dispose();

    TestObserver<String> observer2 = new TestObserver<>();
    observable.subscribe(observer2);
    observer2.assertValues("Foo");
  }

  @Test public void valueMissedWhenNoSubscribers() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    observable.subscribe(observer1);
    observer1.assertNoValues();
    observer1.dispose();

    subject.onNext("Foo");
    observer1.assertNoValues();

    TestObserver<String> observer2 = new TestObserver<>();
    observable.subscribe(observer2);
    observer2.assertNoValues();
  }

  @SuppressWarnings("CheckReturnValue")
  @Test public void fatalExceptionDuringReplayThrown() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    observable.subscribe();
    subject.onNext("Foo");

    Consumer<String> brokenAction = new Consumer<String>() {
      @Override public void accept(String s) {
        throw new OutOfMemoryError("broken!");
      }
    };
    try {
      observable.subscribe(brokenAction);
    } catch (OutOfMemoryError e) {
      assertEquals("broken!", e.getMessage());
    }
  }

  @Test public void refCountToUpstream() {
    PublishSubject<String> subject = PublishSubject.create();

    final AtomicInteger count = new AtomicInteger();
    Observable<String> observable = subject //
        .doOnSubscribe(new Consumer<Disposable>() {
          @Override public void accept(Disposable disposable) throws Exception {
            count.incrementAndGet();
          }
        }) //
        .doOnDispose(new Action() {
          @Override public void run() throws Exception {
            count.decrementAndGet();
          }
        }) //
        .compose(ReplayingShare.<String>instance());

    Disposable disposable1 = observable.subscribeWith(new TestObserver<String>());
    assertEquals(1, count.get());

    Disposable disposable2 = observable.subscribeWith(new TestObserver<String>());
    assertEquals(1, count.get());

    Disposable disposable3 = observable.subscribeWith(new TestObserver<String>());
    assertEquals(1, count.get());

    disposable1.dispose();
    assertEquals(1, count.get());

    disposable3.dispose();
    assertEquals(1, count.get());

    disposable2.dispose();
    assertEquals(0, count.get());
  }

  @Ignore("No backpressure in Observable")
  @Test public void backpressureHonoredWhenCached() {
    PublishSubject<String> subject = PublishSubject.create();
    Observable<String> observable = subject.compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    observable.subscribe(observer1);
    observer1.assertNoValues();

    subject.onNext("Foo");
    observer1.assertValues("Foo");

    TestObserver<String> observer2 = new TestObserver<>(/*0*/);
    observable.subscribe(observer2);
    observer2.assertNoValues();

    subject.onNext("Bar"); // Replace the cached value...
    observer1.assertValues("Foo", "Bar");

    //observer2.requestMore(1); // ...and ensure new requests see it.
    observer2.assertValues("Bar");
  }

  @Test public void streamsDoNotShareInstances() {
    PublishSubject<String> subjectA = PublishSubject.create();
    Observable<String> observableA = subjectA.compose(ReplayingShare.<String>instance());
    TestObserver<String> observerA1 = new TestObserver<>();
    observableA.subscribe(observerA1);

    PublishSubject<String> subjectB = PublishSubject.create();
    Observable<String> observableB = subjectB.compose(ReplayingShare.<String>instance());
    TestObserver<String> observerB1 = new TestObserver<>();
    observableB.subscribe(observerB1);

    subjectA.onNext("Foo");
    observerA1.assertValues("Foo");
    subjectB.onNext("Bar");
    observerB1.assertValues("Bar");

    TestObserver<String> observerA2 = new TestObserver<>();
    observableA.subscribe(observerA2);
    observerA2.assertValues("Foo");

    TestObserver<String> observerB2 = new TestObserver<>();
    observableB.subscribe(observerB2);
    observerB2.assertValues("Bar");
  }

  @Test public void completeClearsCacheAndResubscribes() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishSubject<String> upstream = PublishSubject.create();
    Observable<String> replayed = upstream.startWith(start).compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    replayed.subscribe(observer1);
    observer1.assertValues("initA");

    TestObserver<String> observer2 = new TestObserver<>();
    replayed.subscribe(observer2);
    observer1.assertValues("initA");

    upstream.onComplete();
    observer1.assertComplete();
    observer2.assertComplete();

    start.set(0, "initB");

    TestObserver<String> observer3 = new TestObserver<>();
    replayed.subscribe(observer3);
    observer3.assertValues("initB");
  }

  @Test public void errorClearsCacheAndResubscribes() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishSubject<String> upstream = PublishSubject.create();
    Observable<String> replayed = upstream.startWith(start).compose(ReplayingShare.<String>instance());

    TestObserver<String> observer1 = new TestObserver<>();
    replayed.subscribe(observer1);
    observer1.assertValues("initA");

    TestObserver<String> observer2 = new TestObserver<>();
    replayed.subscribe(observer2);
    observer1.assertValues("initA");

    RuntimeException r = new RuntimeException();
    upstream.onError(r);
    observer1.assertError(r);
    observer2.assertError(r);

    start.set(0, "initB");

    TestObserver<String> observer3 = new TestObserver<>();
    replayed.subscribe(observer3);
    observer3.assertValues("initB");
  }

  @Test public void unsubscribeBeforeSubscribePreventsCacheEmission() {
    PublishSubject<String> upstream = PublishSubject.create();
    Observable<String> replayed = upstream.compose(ReplayingShare.<String>instance());
    replayed.subscribe();
    upstream.onNext("something to cache");

    TestObserver<String> testObserver = new TestObserver<>();
    testObserver.dispose();
    replayed.subscribe(testObserver);
    testObserver.assertNoValues();
  }
}
