/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.osgi

import language.postfixOps

import org.scalatest.WordSpec
import akka.actor.ActorSystem
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.immutable
import akka.util.Timeout
import de.kalpatec.pojosr.framework.launch.BundleDescriptor
import test.{ RuntimeNameActorSystemActivator, TestActivators, PingPongActorSystemActivator }
import test.PingPong._
import PojoSRTestSupport.bundle
import org.scalatest.Matchers

/**
 * Test cases for [[akka.osgi.ActorSystemActivator]] in 2 different scenarios:
 * - no name configured for [[akka.actor.ActorSystem]]
 * - runtime name configuration
 */
object ActorSystemActivatorTest {

  val TEST_BUNDLE_NAME = "akka.osgi.test.activator"

}

class PingPongActorSystemActivatorTest extends WordSpec with Matchers with PojoSRTestSupport {

  import ActorSystemActivatorTest._

  val testBundles: immutable.Seq[BundleDescriptor] = buildTestBundles(List(
    bundle(TEST_BUNDLE_NAME).withActivator(classOf[PingPongActorSystemActivator])))

  "PingPongActorSystemActivator" must {

    "start and register the ActorSystem when bundle starts" in {
      filterErrors() {
        val system = serviceForType[ActorSystem]
        val actor = system.actorSelection("/user/pong")

        implicit val timeout = Timeout(5 seconds)
        Await.result(actor ? Ping, timeout.duration) should be(Pong)
      }
    }

    "stop the ActorSystem when bundle stops" in {
      filterErrors() {
        val system = serviceForType[ActorSystem]
        system.isTerminated should be(false)

        bundleForName(TEST_BUNDLE_NAME).stop()
        system.awaitTermination()
        system.isTerminated should be(true)
      }
    }
  }

}

class RuntimeNameActorSystemActivatorTest extends WordSpec with Matchers with PojoSRTestSupport {

  import ActorSystemActivatorTest._

  val testBundles: immutable.Seq[BundleDescriptor] =
    buildTestBundles(List(bundle(TEST_BUNDLE_NAME).withActivator(classOf[RuntimeNameActorSystemActivator])))

  "RuntimeNameActorSystemActivator" must {

    "register an ActorSystem and add the bundle id to the system name" in {
      filterErrors() {
        serviceForType[ActorSystem].name should be(TestActivators.ACTOR_SYSTEM_NAME_PATTERN.format(bundleForName(TEST_BUNDLE_NAME).getBundleId))
      }
    }
  }

}
