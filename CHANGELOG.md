# Change Log

## Unreleased

Minimum-supported Java version is now 1.8.


## Version 3.0.0 *(2020-05-19)*

This version updates to support RxJava 3. No functional changes.

The groupId and package name are now `com.jakewharton.rx3`.


## Version 2.2.0 *(2019-09-13)*

 * New: `createWithDefault` factory for supplying a default cached value. Currently, the first
   subscriber has to wait for the upstream source to emit. By specifying a default value you can
   deliver a value immediately upon subscribe in a way similar to that if an event from another
   subscriber had already been cached.
 * New: Annotate public API surface with nullability annotations.


## Version 2.1.1 *(2019-02-19)*

 * Fix: Do not emit cached value if the subscriber immediately disposes/cancel subscription.


## Version 2.1.0 *(2018-09-22)*

 * Fix: Clear cached value when upstream emits a terminal event.


## Version 2.0.1 *(2017-09-14)*

 * Fix: Kotlin extension functions now return non-nullable types instead of platform types.


## Version 2.0.0 *(2017-07-02)*

This version only supports RxJava 2.

 * New: Support for both `Observable` and `Flowable`.

Note: There is a 2.0.0 version which uses the `com.jakewharton.rx` group ID instead of `com.jakewharton.rx2`.
You should use the latter to ensure 1.x and 2.x can be used side-by-side in the same module.


## Version 1.0.1 *(2016-03-31)*

 * Fix: Reduce allocations and memory overhead.


## Version 1.0.0 *(2016-03-01)*

Initial release.
