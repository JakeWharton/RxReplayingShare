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

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.subscribers.TestSubscriber;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.junit.Assert.assertEquals;

public final class ReplayingShareFlowableTest {
  @Test public void noInitialValue() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber = new TestSubscriber<>();
    flowable.subscribe(subscriber);
    subscriber.assertNoValues();
  }

  @Test public void initialValueToNewSubscriber() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertNoValues();

    subject.onNext("Foo");
    subscriber1.assertValues("Foo");

    TestSubscriber<String> subscriber2 = new TestSubscriber<>();
    flowable.subscribe(subscriber2);
    subscriber2.assertValues("Foo");
  }

  @Test public void initialValueToNewSubscriberAfterUnsubscribe() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertNoValues();

    subject.onNext("Foo");
    subscriber1.assertValues("Foo");
    subscriber1.dispose();

    TestSubscriber<String> subscriber2 = new TestSubscriber<>();
    flowable.subscribe(subscriber2);
    subscriber2.assertValues("Foo");
  }

  @Test public void valueMissedWhenNoSubscribers() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertNoValues();
    subscriber1.dispose();

    subject.onNext("Foo");
    subscriber1.assertNoValues();

    TestSubscriber<String> subscriber2 = new TestSubscriber<>();
    flowable.subscribe(subscriber2);
    subscriber2.assertNoValues();
  }

  @SuppressWarnings("CheckReturnValue")
  @Test public void fatalExceptionDuringReplayThrown() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    flowable.subscribe();
    subject.onNext("Foo");

    Consumer<String> brokenAction = new Consumer<String>() {
      @Override public void accept(String s) {
        throw new OutOfMemoryError("broken!");
      }
    };
    try {
      flowable.subscribe(brokenAction);
    } catch (OutOfMemoryError e) {
      assertEquals("broken!", e.getMessage());
    }
  }

  @Test public void refCountToUpstream() {
    PublishProcessor<String> subject = PublishProcessor.create();

    final AtomicInteger count = new AtomicInteger();
    Flowable<String> flowable = subject //
        .doOnSubscribe(new Consumer<Subscription>() {
          @Override public void accept(Subscription subscription) {
            count.incrementAndGet();
          }
        }) //
        .doOnCancel(new Action() {
          @Override public void run() {
            count.decrementAndGet();
          }
        }) //
        .compose(ReplayingShare.<String>instance());

    Disposable disposable1 = flowable.subscribeWith(new TestSubscriber<String>());
    assertEquals(1, count.get());

    Disposable disposable2 = flowable.subscribeWith(new TestSubscriber<String>());
    assertEquals(1, count.get());

    Disposable disposable3 = flowable.subscribeWith(new TestSubscriber<String>());
    assertEquals(1, count.get());

    disposable1.dispose();
    assertEquals(1, count.get());

    disposable3.dispose();
    assertEquals(1, count.get());

    disposable2.dispose();
    assertEquals(0, count.get());
  }

  @Test public void backpressureHonoredWhenCached() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertNoValues();

    subject.onNext("Foo");
    subscriber1.assertValues("Foo");

    TestSubscriber<String> subscriber2 = new TestSubscriber<>(0);
    flowable.subscribe(subscriber2);
    subscriber2.assertNoValues();

    subject.onNext("Bar"); // Replace the cached value...
    subscriber2.request(1); // ...and ensure new requests see it.
    subscriber2.assertValues("Bar");
  }

  @Test public void streamsDoNotShareInstances() {
    PublishProcessor<String> subjectA = PublishProcessor.create();
    Flowable<String> flowableA = subjectA.compose(ReplayingShare.<String>instance());
    TestSubscriber<String> subscriberA1 = new TestSubscriber<>();
    flowableA.subscribe(subscriberA1);

    PublishProcessor<String> subjectB = PublishProcessor.create();
    Flowable<String> flowableB = subjectB.compose(ReplayingShare.<String>instance());
    TestSubscriber<String> subscriberB1 = new TestSubscriber<>();
    flowableB.subscribe(subscriberB1);

    subjectA.onNext("Foo");
    subscriberA1.assertValues("Foo");
    subjectB.onNext("Bar");
    subscriberB1.assertValues("Bar");

    TestSubscriber<String> subscriberA2 = new TestSubscriber<>();
    flowableA.subscribe(subscriberA2);
    subscriberA2.assertValues("Foo");

    TestSubscriber<String> subscriberB2 = new TestSubscriber<>();
    flowableB.subscribe(subscriberB2);
    subscriberB2.assertValues("Bar");
  }

  @Test public void completeClearsCacheAndResubscribes() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishProcessor<String> upstream = PublishProcessor.create();
    Flowable<String> replayed = upstream.startWith(start).compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    replayed.subscribe(subscriber1);
    subscriber1.assertValues("initA");

    TestSubscriber<String> observer2 = new TestSubscriber<>();
    replayed.subscribe(observer2);
    subscriber1.assertValues("initA");

    upstream.onComplete();
    subscriber1.assertComplete();
    observer2.assertComplete();

    start.set(0, "initB");

    TestSubscriber<String> observer3 = new TestSubscriber<>();
    replayed.subscribe(observer3);
    observer3.assertValues("initB");
  }

  @Test public void errorClearsCacheAndResubscribes() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishProcessor<String> upstream = PublishProcessor.create();
    Flowable<String> replayed = upstream.startWith(start).compose(ReplayingShare.<String>instance());

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    replayed.subscribe(subscriber1);
    subscriber1.assertValues("initA");

    TestSubscriber<String> observer2 = new TestSubscriber<>();
    replayed.subscribe(observer2);
    subscriber1.assertValues("initA");

    RuntimeException r = new RuntimeException();
    upstream.onError(r);
    subscriber1.assertError(r);
    observer2.assertError(r);

    start.set(0, "initB");

    TestSubscriber<String> observer3 = new TestSubscriber<>();
    replayed.subscribe(observer3);
    observer3.assertValues("initB");
  }

  @Test public void unsubscribeBeforeSubscribePreventsCacheEmission() {
    PublishProcessor<String> upstream = PublishProcessor.create();
    Flowable<String> replayed = upstream.compose(ReplayingShare.<String>instance());
    replayed.subscribe();
    upstream.onNext("something to cache");

    TestSubscriber<String> testSubscriber = new TestSubscriber<>();
    testSubscriber.cancel();
    replayed.subscribe(testSubscriber);
    testSubscriber.assertNoValues();
  }

  @Test public void defaultValueOnSubscribe() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.createWithDefault("default"));

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertValues("default");

    subject.onNext("Foo");
    subscriber1.assertValues("default", "Foo");
  }

  @Test public void defaultValueIsOverriddenByLatestEmissionForNewSubscriber() {
    PublishProcessor<String> subject = PublishProcessor.create();
    Flowable<String> flowable = subject.compose(ReplayingShare.createWithDefault("default"));

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    flowable.subscribe(subscriber1);
    subscriber1.assertValues("default");

    subject.onNext("Foo");
    subscriber1.assertValues("default", "Foo");

    TestSubscriber<String> observer2 = new TestSubscriber<>();
    flowable.subscribe(observer2);
    observer2.assertValues("Foo");
  }

  @Test public void completeClearsCacheAndResubscribesStartingWithDefault() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishProcessor<String> upstream = PublishProcessor.create();
    Flowable<String> replayed =
        upstream.startWith(start).compose(ReplayingShare.createWithDefault("default"));

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    replayed.subscribe(subscriber1);
    subscriber1.assertValues("default", "initA");

    TestSubscriber<String> observer2 = new TestSubscriber<>();
    replayed.subscribe(observer2);
    subscriber1.assertValues("default", "initA");

    upstream.onComplete();
    subscriber1.assertComplete();
    observer2.assertComplete();

    start.set(0, "initB");

    TestSubscriber<String> observer3 = new TestSubscriber<>();
    replayed.subscribe(observer3);
    observer3.assertValues("default", "initB");
  }

  @Test public void errorClearsCacheAndResubscribesStartingWithDefault() {
    List<String> start = new ArrayList<>();
    start.add("initA");

    PublishProcessor<String> upstream = PublishProcessor.create();
    Flowable<String> replayed =
        upstream.startWith(start).compose(ReplayingShare.createWithDefault("default"));

    TestSubscriber<String> subscriber1 = new TestSubscriber<>();
    replayed.subscribe(subscriber1);
    subscriber1.assertValues("default", "initA");

    TestSubscriber<String> observer2 = new TestSubscriber<>();
    replayed.subscribe(observer2);
    subscriber1.assertValues("default", "initA");

    RuntimeException r = new RuntimeException();
    upstream.onError(r);
    subscriber1.assertError(r);
    observer2.assertError(r);

    start.set(0, "initB");

    TestSubscriber<String> observer3 = new TestSubscriber<>();
    replayed.subscribe(observer3);
    observer3.assertValues("default", "initB");
  }
}
