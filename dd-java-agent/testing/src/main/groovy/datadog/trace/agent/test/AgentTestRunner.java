package datadog.trace.agent.test;

import static datadog.trace.api.IdGenerationStrategy.THREAD_PREFIX;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.util.ContextInitializer;
import com.google.common.collect.Sets;
import com.timgroup.statsd.StatsDClient;
import datadog.trace.agent.test.asserts.ListWriterAssert;
import datadog.trace.agent.tooling.AgentInstaller;
import datadog.trace.agent.tooling.Instrumenter;
import datadog.trace.agent.tooling.TracerInstaller;
import datadog.trace.agent.tooling.bytebuddy.matcher.AdditionalLibraryIgnoresMatcher;
import datadog.trace.api.Config;
import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import datadog.trace.bootstrap.instrumentation.api.AgentTracer.TracerAPI;
import datadog.trace.common.writer.ListWriter;
import datadog.trace.common.writer.Writer;
import datadog.trace.core.CoreTracer;
import datadog.trace.core.DDSpan;
import datadog.trace.core.PendingTrace;
import datadog.trace.util.test.DDSpecification;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import groovy.transform.stc.ClosureParams;
import groovy.transform.stc.SimpleType;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.utility.JavaModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.spockframework.mock.MockUtil;
import org.spockframework.runtime.model.SpecMetadata;
import spock.mock.DetachedMockFactory;

/**
 * A spock test runner which automatically applies instrumentation and exposes a global trace
 * writer.
 *
 * <p>To use, write a regular spock test, but extend this class instead of {@link
 * spock.lang.Specification}. <br>
 * This will cause the following to occur before test startup:
 *
 * <ul>
 *   <li>All {@link Instrumenter}s on the test classpath will be applied. Matching preloaded classes
 *       will be retransformed.
 *   <li>{@link AgentTestRunner#TEST_WRITER} will be registerd with the global tracer and available
 *       in an initialized state.
 * </ul>
 */
@RunWith(SpockRunner.class)
@SpecMetadata(filename = "AgentTestRunner.java", line = 0)
@Slf4j
public abstract class AgentTestRunner extends DDSpecification {
  private static final long TIMEOUT_MILLIS = 10 * 1000;
  /**
   * For test runs, agent's global tracer will report to this list writer.
   *
   * <p>Before the start of each test the reported traces will be reset.
   */
  public static final ListWriter TEST_WRITER;

  // having a reference to io.opentracing.Tracer in test field
  // loads opentracing before bootstrap classpath is setup
  // so we declare tracer as an object and cast when needed.
  protected static final Object TEST_TRACER;

  protected static final StatsDClient STATS_D_CLIENT;

  private static final ElementMatcher.Junction<TypeDescription> GLOBAL_LIBRARIES_IGNORES_MATCHER =
      AdditionalLibraryIgnoresMatcher.additionalLibraryIgnoresMatcher();

  protected static final Set<String> TRANSFORMED_CLASSES_NAMES = Sets.newConcurrentHashSet();
  protected static final Set<TypeDescription> TRANSFORMED_CLASSES_TYPES =
      Sets.newConcurrentHashSet();
  private static final AtomicInteger INSTRUMENTATION_ERROR_COUNT = new AtomicInteger(0);
  private static final TestRunnerListener TEST_LISTENER = new TestRunnerListener();

  private static final Instrumentation INSTRUMENTATION;
  private static volatile ClassFileTransformer activeTransformer = null;

  static {
    // If this fails, it's likely the result of another test loading Config before it can be
    // injected into the bootstrap classpath.
    assert Config.class.getClassLoader() == null : "Config must load on the bootstrap classpath.";
    INSTRUMENTATION = ByteBuddyAgent.getInstrumentation();

    configureLoggingLevels();

    TEST_WRITER =
        new ListWriter() {
          @Override
          public boolean add(final List<DDSpan> trace) {
            final boolean result = super.add(trace);
            return result;
          }
        };

    STATS_D_CLIENT = new DetachedMockFactory().Mock(StatsDClient.class);
    TEST_TRACER =
        CoreTracer.builder()
            .writer(TEST_WRITER)
            .idGenerationStrategy(THREAD_PREFIX)
            .statsDClient(STATS_D_CLIENT)
            .build();
    TracerInstaller.installGlobalTracer((CoreTracer) TEST_TRACER);
  }

  private static void configureLoggingLevels() {
    ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
    ((Logger) LoggerFactory.getLogger("datadog")).setLevel(Level.DEBUG);
  }

  protected static TracerAPI getTestTracer() {
    return (TracerAPI) TEST_TRACER;
  }

  protected static Writer getTestWriter() {
    return TEST_WRITER;
  }

  /**
   * Invoked when Bytebuddy encounters an instrumentation error. Fails the test by default.
   *
   * <p>Override to skip specific expected errors.
   *
   * @return true if the test should fail because of this error.
   */
  protected boolean onInstrumentationError(
      final String typeName,
      final ClassLoader classLoader,
      final JavaModule module,
      final boolean loaded,
      final Throwable throwable) {
    log.error(
        "Unexpected instrumentation error when instrumenting {} on {}",
        typeName,
        classLoader,
        throwable);
    return true;
  }

  /**
   * @param className name of the class being loaded
   * @param classLoader classloader class is being defined on
   * @return true if the class under load should be transformed for this test.
   */
  protected boolean shouldTransformClass(final String className, final ClassLoader classLoader) {
    return true;
  }

  @BeforeClass
  public static synchronized void agentSetup() {
    if (null != activeTransformer) {
      throw new IllegalStateException("transformer already in place: " + activeTransformer);
    }
    assert ServiceLoader.load(Instrumenter.class, AgentTestRunner.class.getClassLoader())
            .iterator()
            .hasNext()
        : "No instrumentation found";
    activeTransformer = AgentInstaller.installBytebuddyAgent(INSTRUMENTATION, true, TEST_LISTENER);
  }

  /**
   * Normally {@code @BeforeClass} is run only on static methods, but spock allows us to run it on
   * instance methods. Note: this means there is a 'special' instance of test class that is not used
   * to run any tests, but instead is just used to run this method once.
   */
  @BeforeClass
  public void setupBeforeTests() {
    TEST_LISTENER.activateTest(this);
  }

  @Before
  public void beforeTest() {
    checkLoggingConfiguration();
    assert getTestTracer().activeSpan() == null
        : "Span is active before test has started: " + getTestTracer().activeSpan();
    log.debug("Starting test: '{}'", getSpecificationContext().getCurrentIteration().getName());
    new MockUtil().attachMock(STATS_D_CLIENT, this);
    TEST_WRITER.start();
  }

  private static void checkLoggingConfiguration() {
    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (!rootLogger.iteratorForAppenders().hasNext()) {
      try {
        // previous test wiped out the logging config; bring it back for the next test
        new ContextInitializer(rootLogger.getLoggerContext()).autoConfig();
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }
    configureLoggingLevels();
  }

  @After
  public void detachAfter() {
    new MockUtil().detachMock(STATS_D_CLIENT);
  }

  /** See comment for {@code #setupBeforeTests} above. */
  @AfterClass
  public void cleanUpAfterTests() {
    TEST_LISTENER.deactivateTest(this);
  }

  @AfterClass
  public static synchronized void agentCleanup() {
    if (null != activeTransformer) {
      INSTRUMENTATION.removeTransformer(activeTransformer);
      activeTransformer = null;
    }
    // Cleanup before assertion.
    assert INSTRUMENTATION_ERROR_COUNT.get() == 0
        : INSTRUMENTATION_ERROR_COUNT.get() + " Instrumentation errors during test";

    final List<TypeDescription> ignoredClassesTransformed = new ArrayList<>();
    for (final TypeDescription type : TRANSFORMED_CLASSES_TYPES) {
      if (GLOBAL_LIBRARIES_IGNORES_MATCHER.matches(type)) {
        ignoredClassesTransformed.add(type);
      }
    }
    assert ignoredClassesTransformed.isEmpty()
        : "Transformed classes match global libraries ignore matcher: " + ignoredClassesTransformed;
  }

  public static void assertTraces(
      final int size,
      @ClosureParams(
              value = SimpleType.class,
              options = "datadog.trace.agent.test.asserts.ListWriterAssert")
          @DelegatesTo(value = ListWriterAssert.class, strategy = Closure.DELEGATE_FIRST)
          final Closure spec) {
    assertTraces(size, false, spec);
  }

  public static void assertTraces(
      final int size,
      final boolean ignoreAdditionalTraces,
      @ClosureParams(
              value = SimpleType.class,
              options = "datadog.trace.agent.test.asserts.ListWriterAssert")
          @DelegatesTo(value = ListWriterAssert.class, strategy = Closure.DELEGATE_FIRST)
          final Closure spec) {
    ListWriterAssert.assertTraces(TEST_WRITER, size, ignoreAdditionalTraces, spec);
  }

  @SneakyThrows
  public static void blockUntilChildSpansFinished(final int numberOfSpans) {
    final AgentSpan span = getTestTracer().activeSpan();
    final long deadline = System.currentTimeMillis() + TIMEOUT_MILLIS;
    if (span instanceof DDSpan) {
      final PendingTrace pendingTrace = ((DDSpan) span).context().getTrace();

      while (pendingTrace.size() < numberOfSpans) {
        if (System.currentTimeMillis() > deadline) {
          throw new TimeoutException(
              "Timed out waiting for child spans.  Received: " + pendingTrace.size());
        }
        Thread.sleep(10);
      }
    }
  }

  public static class TestRunnerListener implements AgentBuilder.Listener {
    private static final List<AgentTestRunner> activeTests = new CopyOnWriteArrayList<>();

    public void activateTest(final AgentTestRunner testRunner) {
      activeTests.add(testRunner);
    }

    public void deactivateTest(final AgentTestRunner testRunner) {
      activeTests.remove(testRunner);
    }

    @Override
    public void onDiscovery(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded) {
      for (final AgentTestRunner testRunner : activeTests) {
        if (!testRunner.shouldTransformClass(typeName, classLoader)) {
          throw new AbortTransformationException(
              "Aborting transform for class name = " + typeName + ", loader = " + classLoader);
        }
      }
    }

    @Override
    public void onTransformation(
        final TypeDescription typeDescription,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded,
        final DynamicType dynamicType) {
      TRANSFORMED_CLASSES_NAMES.add(typeDescription.getActualName());
      TRANSFORMED_CLASSES_TYPES.add(typeDescription);
    }

    @Override
    public void onIgnored(
        final TypeDescription typeDescription,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded) {}

    @Override
    public void onError(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded,
        final Throwable throwable) {
      if (!(throwable instanceof AbortTransformationException)) {
        for (final AgentTestRunner testRunner : activeTests) {
          if (testRunner.onInstrumentationError(typeName, classLoader, module, loaded, throwable)) {
            INSTRUMENTATION_ERROR_COUNT.incrementAndGet();
            break;
          }
        }
      }
    }

    @Override
    public void onComplete(
        final String typeName,
        final ClassLoader classLoader,
        final JavaModule module,
        final boolean loaded) {}

    /** Used to signal that a transformation was intentionally aborted and is not an error. */
    public static class AbortTransformationException extends RuntimeException {
      public AbortTransformationException() {
        super();
      }

      public AbortTransformationException(final String message) {
        super(message);
      }
    }
  }

  protected static String getClassName(final Class clazz) {
    String className = clazz.getSimpleName();
    if (className.isEmpty()) {
      className = clazz.getName();
      if (clazz.getPackage() != null) {
        final String pkgName = clazz.getPackage().getName();
        if (!pkgName.isEmpty()) {
          className = clazz.getName().replace(pkgName, "").substring(1);
        }
      }
    }
    return className;
  }
}
