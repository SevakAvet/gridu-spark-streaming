import BotDetectorV1.{EventAggregate, LogEvent}
import org.scalatest.{FlatSpec, Matchers}

class TestBotDetectorV1 extends FlatSpec with Matchers {
  "Reduce event function" should "add corresponding fields of two event to each other" in {
    val firstEvent = EventAggregate(ip = "127.0.0.1", eventRate = 1, clickRate = 1, viewRate = 0, categories = Set(2))
    val secondEvent = EventAggregate(ip = "127.0.0.1", eventRate = 1, clickRate = 0, viewRate = 1, categories = Set(1))

    val reducedEvent = BotDetectorV1.reduceEvent(firstEvent, secondEvent)

    reducedEvent.eventRate should be(2)
    reducedEvent.clickRate should be(1)
    reducedEvent.viewRate should be(1)
    reducedEvent.categories should be(Set(1, 2))
  }

  "Reduce event function" should "not merge two sets, if one of them already exceeded rate limit" in {
    val firstEvent = EventAggregate(ip = "127.0.0.1", eventRate = 1, clickRate = 1, viewRate = 0, categories = Set(1, 2, 3))
    val secondEvent = EventAggregate(ip = "127.0.0.1", eventRate = 1, clickRate = 0, viewRate = 1, categories = Set(4))

    val reducedEvent = BotDetectorV1.reduceEvent(firstEvent, secondEvent)

    reducedEvent.categories should be(Set(1, 2, 3))
  }

  "Build event aggregate from LogEvent with 'click' type" should "return an object with clickRate = 1" in {
    val event = LogEvent(unix_time = 12312312, category_id = 1, ip = "127.0.0.1", `type` = "click")
    val aggregate = BotDetectorV1.buildEventAggregate(event)

    aggregate.clickRate should be(1)
    aggregate.viewRate should be(0)
    aggregate.eventRate should be(1)
  }

  "Build event aggregate from LogEvent with 'view' type" should "return an object with clickRate = 1" in {
    val event = LogEvent(unix_time = 12312312, category_id = 1, ip = "127.0.0.1", `type` = "view")
    val aggregate = BotDetectorV1.buildEventAggregate(event)

    aggregate.viewRate should be(1)
    aggregate.clickRate should be(0)
    aggregate.eventRate should be(1)
  }

  "Bot rules list" should "identify bots in event aggregates list" in {
    // viewRate is 0
    val bot1 = EventAggregate(ip = "127.0.0.1", eventRate = 1, clickRate = 1, viewRate = 0, categories = Set(1))
    // categories limit exceeded
    val bot2 = EventAggregate(ip = "127.0.0.2", eventRate = 2, clickRate = 1, viewRate = 1, categories = Set(2, 3))

    // event rate exceeded
    val bot3 = EventAggregate(ip = "127.0.0.3", eventRate = 11, clickRate = 5, viewRate = 5, categories = Set(3))

    // click/view rate exceeded
    val bot4 = EventAggregate(ip = "127.0.0.4", eventRate = 5, clickRate = 4, viewRate = 1, categories = Set(3))

    val events = Array(
      bot1,
      bot2,
      bot3,
      bot4,
      EventAggregate(ip = "127.0.0.5", eventRate = 1, clickRate = 1, viewRate = 1, categories = Set(1)),
      EventAggregate(ip = "127.0.0.6", eventRate = 1, clickRate = 1, viewRate = 1, categories = Set(1)),
      EventAggregate(ip = "127.0.0.7", eventRate = 1, clickRate = 1, viewRate = 1, categories = Set(1))
    )

    val bots = events.filter(BotDetectorV1.or(BotDetectorV1.botRules))

    bots should contain allOf(bot1, bot2, bot3, bot4)
  }
}
