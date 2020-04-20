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
package com.jakewharton.rx3

import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable

/**
 * A transformer which combines `replay(1)`, `publish()`, and `refCount()` operators.
 *
 * Unlike traditional combinations of these operators, `ReplayingShare` caches the last emitted
 * value from the upstream observable *only* when one or more downstream observers are connected.
 * This allows expensive upstream observables to be shut down when no one is observing while also
 * replaying the last value seen by *any* observer to new ones.
 *
 * @param defaultValue the initial value delivered to new subscribers before any events are cached.
 * A null value means there will be no initial emission.
 */
@JvmOverloads
fun <T> Observable<T>.replayingShare(defaultValue: T? = null): Observable<T> {
  return compose(
      if (defaultValue != null) ReplayingShare.createWithDefault(defaultValue)
      else ReplayingShare.instance<T>()
  )
}

/**
 * A transformer which combines `replay(1)`, `publish()`, and `refCount()` operators.
 *
 * Unlike traditional combinations of these operators, `ReplayingShare` caches the last emitted
 * value from the upstream flowable *only* when one or more downstream subscribers are connected.
 * This allows expensive upstream flowables to be shut down when no one is subscribed while also
 * replaying the last value seen by *any* subscriber to new ones.
 *
 * @param defaultValue the initial value delivered to new subscribers before any events are cached.
 * A null value means there will be no initial emission.
 */
@JvmOverloads
fun <T> Flowable<T>.replayingShare(defaultValue: T? = null): Flowable<T> {
  return compose(
      if (defaultValue != null) ReplayingShare.createWithDefault(defaultValue)
      else ReplayingShare.instance<T>()
  )
}
