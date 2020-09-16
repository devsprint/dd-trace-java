import datadog.trace.agent.test.AgentTestRunner
import datadog.trace.agent.test.asserts.ListWriterAssert
import datadog.trace.api.Trace
import datadog.trace.bootstrap.instrumentation.api.AgentScope
import datadog.trace.bootstrap.instrumentation.api.AgentSpan
import datadog.trace.bootstrap.instrumentation.api.Tags
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Shared

import java.time.Duration

import static datadog.trace.agent.test.utils.TraceUtils.basicSpan
import static datadog.trace.agent.test.utils.TraceUtils.runUnderTrace
import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.activateSpan
import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.startSpan

class ReactorCoreTest extends AgentTestRunner {

  public static final String EXCEPTION_MESSAGE = "test exception"

  @Shared
  def addOne = { i ->
    addOneFunc(i)
  }

  @Shared
  def addTwo = { i ->
    addTwoFunc(i)
  }

  @Shared
  def throwException = {
    throw new RuntimeException(EXCEPTION_MESSAGE)
  }

  def "Publisher '#name' test"() {
    when:
    def result = assemblePublisherUnderTrace(publisherSupplier)

    then:
    result == expected
    and:
    sortAndAssertTraces(1) {
      trace(0, workSpans + 2) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }

        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))

        for (int i = 0; i < workSpans; i++) {
          span(i + 2) {
            resourceName "addOne"
            operationName "addOne"
            childOf(span(1))
            tags {
              "$Tags.COMPONENT" "trace"
              defaultTags()
            }
          }
        }
      }
    }

    where:
    name                  | expected | workSpans | publisherSupplier
    "basic mono"          | 2        | 1         | { -> Mono.just(1).map(addOne) }
    "two operations mono" | 4        | 2         | { -> Mono.just(2).map(addOne).map(addOne) }
    "delayed mono"        | 4        | 1         | { -> Mono.just(3).delayElement(Duration.ofMillis(100)).map(addOne) }
    "delayed twice mono"  | 6        | 2         | { ->
      Mono.just(4).delayElement(Duration.ofMillis(100)).map(addOne).delayElement(Duration.ofMillis(100)).map(addOne)
    }
    "basic flux"          | [6, 7]   | 2         | { -> Flux.fromIterable([5, 6]).map(addOne) }
    "two operations flux" | [8, 9]   | 4         | { -> Flux.fromIterable([6, 7]).map(addOne).map(addOne) }
    "delayed flux"        | [8, 9]   | 2         | { ->
      Flux.fromIterable([7, 8]).delayElements(Duration.ofMillis(100)).map(addOne)
    }
    "delayed twice flux"  | [10, 11] | 4         | { ->
      Flux.fromIterable([8, 9]).delayElements(Duration.ofMillis(100)).map(addOne).delayElements(Duration.ofMillis(100)).map(addOne)
    }

    "mono from callable"  | 12       | 2         | { -> Mono.fromCallable({ addOneFunc(10) }).map(addOne) }
  }

  def "Publisher error '#name' test"() {
    when:
    assemblePublisherUnderTrace(publisherSupplier)

    then:
    def exception = thrown RuntimeException
    exception.message == EXCEPTION_MESSAGE
    and:
    sortAndAssertTraces(1) {
      trace(0, 2) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          errored true
          tags {
            "$Tags.COMPONENT" "trace"
            errorTags(RuntimeException, EXCEPTION_MESSAGE)
            defaultTags()
          }
        }

        // It's important that we don't attach errors at the Reactor level so that we don't
        // impact the spans on reactor integrations such as netty and lettuce, as reactor is
        // more of a context propagation mechanism than something we would be tracking for
        // errors this is ok.
        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))
      }
    }

    where:
    name   | publisherSupplier
    "mono" | { -> Mono.error(new RuntimeException(EXCEPTION_MESSAGE)) }
    "flux" | { -> Flux.error(new RuntimeException(EXCEPTION_MESSAGE)) }
  }

  def "Publisher step '#name' test"() {
    when:
    assemblePublisherUnderTrace(publisherSupplier)

    then:
    def exception = thrown RuntimeException
    exception.message == EXCEPTION_MESSAGE
    and:
    sortAndAssertTraces(1) {
      trace(0, workSpans + 2) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          errored true
          tags {
            "$Tags.COMPONENT" "trace"
            errorTags(RuntimeException, EXCEPTION_MESSAGE)
            defaultTags()
          }
        }

        // It's important that we don't attach errors at the Reactor level so that we don't
        // impact the spans on reactor integrations such as netty and lettuce, as reactor is
        // more of a context propagation mechanism than something we would be tracking for
        // errors this is ok.
        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))

        for (int i = 0; i < workSpans; i++) {
          span(i + 2) {
            resourceName "addOne"
            operationName "addOne"
            childOf(span(1))
            tags {
              "$Tags.COMPONENT" "trace"
              defaultTags()
            }
          }
        }
      }
    }

    where:
    name                 | workSpans | publisherSupplier
    "basic mono failure" | 1         | { -> Mono.just(1).map(addOne).map({ throwException() }) }
    "basic flux failure" | 1         | { -> Flux.fromIterable([5, 6]).map(addOne).map({ throwException() }) }
  }

  def "Publisher '#name' cancel"() {
    when:
    cancelUnderTrace(publisherSupplier)

    then:
    assertTraces(1) {
      trace(0, 2) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }

        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))
      }
    }

    where:
    name         | publisherSupplier
    "basic mono" | { -> Mono.just(1) }
    "basic flux" | { -> Flux.fromIterable([5, 6]) }
  }

  def "Publisher chain spans have the correct parent for '#name'"() {
    when:
    assemblePublisherUnderTrace(publisherSupplier)

    then:
    assertTraces(1) {
      trace(0, workSpans + 2) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }

        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))

        for (int i = 0; i < workSpans; i++) {
          span(i + 2) {
            resourceName "addOne"
            operationName "addOne"
            childOf(span(1))
            tags {
              "$Tags.COMPONENT" "trace"
              defaultTags()
            }
          }
        }
      }
    }

    where:
    name         | workSpans | publisherSupplier
    "basic mono" | 3         | { -> Mono.just(1).map(addOne).map(addOne).then(Mono.just(1).map(addOne)) }
    "basic flux" | 5         | { -> Flux.fromIterable([5, 6]).map(addOne).map(addOne).then(Mono.just(1).map(addOne)) }
  }

  def "Publisher chain spans have the correct parents from subscription time"() {
    when:
    def mono = Mono.just(42)
      .map(addOne)
      .map(addTwo)

    runUnderTrace("trace-parent") {
      mono.block()
    }

    then:
    sortAndAssertTraces(1) {
      trace(0, 3) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
        }
        span(1) {
          operationName "addOne"
          childOf span(0)
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }
        span(2) {
          operationName "addTwo"
          childOf span(0)
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }
      }
    }
  }

  def "Publisher chain spans have the correct parents from subscription time '#name'"() {
    when:
    assemblePublisherUnderTrace {
      // The "add one" operations in the publisher created here should be children of the publisher-parent
      Publisher<Integer> publisher = publisherSupplier()

      AgentSpan intermediate = startSpan("intermediate")
      AgentScope scope = activateSpan(intermediate)
      try {
        if (publisher instanceof Mono) {
          return ((Mono) publisher).map(addTwo)
        } else if (publisher instanceof Flux) {
          return ((Flux) publisher).map(addTwo)
        }
        throw new IllegalStateException("Unknown publisher type")
      } finally {
        intermediate.finish()
        scope.close()
      }
    }

    then:
    sortAndAssertTraces(1) {
      trace(0, (workItems * 2) + 3) {
        span(0) {
          resourceName "trace-parent"
          operationName "trace-parent"
          parent()
          tags {
            "$Tags.COMPONENT" "trace"
            defaultTags()
          }
        }

        basicSpan(it, 1, "publisher-parent", "publisher-parent", span(0))
        basicSpan(it, 2, "intermediate", span(1))

        for (int i = 0; i < 2 * workItems; i = i + 2) {
          span(3 + i) {
            operationName "addOne"
            childOf span(1)
            tags {
              "$Tags.COMPONENT" "trace"
              defaultTags()
            }
          }
          span(3 + i + 1) {
            operationName "addTwo"
            childOf span(1)
            tags {
              "$Tags.COMPONENT" "trace"
              defaultTags()
            }
          }
        }
      }
    }

    where:
    name         | workItems | publisherSupplier
    "basic mono" | 1         | { -> Mono.just(1).map(addOne) }
    "basic flux" | 2         | { -> Flux.fromIterable([1, 2]).map(addOne) }
  }

  @Trace(operationName = "trace-parent", resourceName = "trace-parent")
  def assemblePublisherUnderTrace(def publisherSupplier) {
    def span = startSpan("publisher-parent")
    // After this activation, the "add two" operations below should be children of this span
    def scope = activateSpan(span)

    Publisher<Integer> publisher = publisherSupplier()
    try {
      scope.setAsyncPropagation(true)

      // Read all data from publisher
      if (publisher instanceof Mono) {
        return publisher.block()
      } else if (publisher instanceof Flux) {
        return publisher.toStream().toArray({ size -> new Integer[size] })
      }

      throw new RuntimeException("Unknown publisher: " + publisher)
    } finally {
      span.finish()
      scope.close()
    }
  }

  @Trace(operationName = "trace-parent", resourceName = "trace-parent")
  def cancelUnderTrace(def publisherSupplier) {
    final AgentSpan span = startSpan("publisher-parent")
    AgentScope scope = activateSpan(span)
    scope.setAsyncPropagation(true)

    def publisher = publisherSupplier()
    publisher.subscribe(new Subscriber<Integer>() {
      void onSubscribe(Subscription subscription) {
        subscription.cancel()
      }

      void onNext(Integer t) {
      }

      void onError(Throwable error) {
      }

      void onComplete() {
      }
    })

    scope.close()
    span.finish()
  }

  @Trace(operationName = "addOne", resourceName = "addOne")
  def static addOneFunc(int i) {
    return i + 1
  }

  @Trace(operationName = "addTwo", resourceName = "addTwo")
  def static addTwoFunc(int i) {
    return i + 2
  }

  void sortAndAssertTraces(
    final int size,
    @ClosureParams(value = SimpleType, options = "datadog.trace.agent.test.asserts.ListWriterAssert")
    @DelegatesTo(value = ListWriterAssert, strategy = Closure.DELEGATE_FIRST)
    final Closure spec) {

    TEST_WRITER.waitForTraces(size)

    TEST_WRITER.each {
      it.sort({ a, b ->
        return a.startTimeNano <=> b.startTimeNano
      })
    }

    TEST_WRITER.sort({ a, b ->
      return a[0].startTimeNano <=> b[0].startTimeNano
    })

    assertTraces(size, spec)
  }
}
