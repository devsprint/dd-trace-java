import datadog.trace.api.DDSpanTypes
import datadog.trace.bootstrap.instrumentation.api.Tags
import org.hibernate.Criteria
import org.hibernate.Session
import org.hibernate.criterion.Order
import org.hibernate.criterion.Restrictions

class CriteriaTest extends AbstractHibernateTest {

  def "test criteria.#methodName"() {
    setup:
    Session session = sessionFactory.openSession()
    session.beginTransaction()
    Criteria criteria = session.createCriteria(Value)
      .add(Restrictions.like("name", "Hello"))
      .addOrder(Order.desc("name"))
    interaction.call(criteria)
    session.getTransaction().commit()
    session.close()

    expect:
    assertTraces(1) {
      trace(4) {
        span {
          serviceName "hibernate"
          resourceName "hibernate.session"
          operationName "hibernate.session"
          spanType DDSpanTypes.HIBERNATE
          parent()
          tags {
            "$Tags.COMPONENT" "java-hibernate"
            "$Tags.SPAN_KIND" Tags.SPAN_KIND_CLIENT
            defaultTags()
          }
        }
        span {
          serviceName "hibernate"
          resourceName "hibernate.transaction.commit"
          operationName "hibernate.transaction.commit"
          spanType DDSpanTypes.HIBERNATE
          childOf span(0)
          tags {
            "$Tags.COMPONENT" "java-hibernate"
            "$Tags.SPAN_KIND" Tags.SPAN_KIND_CLIENT
            defaultTags()
          }
        }
        span {
          serviceName "hibernate"
          resourceName "hibernate.criteria.$methodName"
          operationName "hibernate.criteria.$methodName"
          spanType DDSpanTypes.HIBERNATE
          childOf span(0)
          tags {
            "$Tags.COMPONENT" "java-hibernate"
            "$Tags.SPAN_KIND" Tags.SPAN_KIND_CLIENT
            defaultTags()
          }
        }
        span {
          serviceName "h2"
          spanType "sql"
          childOf span(2)
          tags {
            "$Tags.COMPONENT" "java-jdbc-prepared_statement"
            "$Tags.SPAN_KIND" Tags.SPAN_KIND_CLIENT
            "$Tags.DB_TYPE" "h2"
            "$Tags.DB_INSTANCE" "db1"
            "$Tags.DB_USER" "sa"
            "span.origin.type" "org.h2.jdbc.JdbcPreparedStatement"
            defaultTags()
          }
        }
      }
    }

    where:
    methodName     | interaction
    "list"         | { c -> c.list() }
    "uniqueResult" | { c -> c.uniqueResult() }
    "scroll"       | { c -> c.scroll() }
  }
}
