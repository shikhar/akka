/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.io

import akka.util.ByteString
import org.reactivestreams.api.{ Processor, Producer, Consumer }
import java.net.InetSocketAddress
import akka.actor._
import scala.collection._
import scala.concurrent.duration.FiniteDuration
import akka.io.Inet.SocketOption
import akka.io.Tcp
import akka.stream.impl.{ ActorPublisher, ExposedPublisher, ActorProcessor }
import akka.stream.MaterializerSettings
import akka.io.IO
import akka.japi.Util

object StreamTcp extends ExtensionId[StreamTcpExt] with ExtensionIdProvider {

  override def lookup = StreamTcp
  override def createExtension(system: ExtendedActorSystem): StreamTcpExt = new StreamTcpExt(system)
  override def get(system: ActorSystem): StreamTcpExt = super.get(system)

  case class OutgoingTcpConnection(remoteAddress: InetSocketAddress,
                                   localAddress: InetSocketAddress,
                                   processor: Processor[ByteString, ByteString]) {
    def outputStream: Consumer[ByteString] = processor
    def inputStream: Producer[ByteString] = processor
  }

  case class TcpServerBinding(localAddress: InetSocketAddress,
                              connectionStream: Producer[IncomingTcpConnection])

  case class IncomingTcpConnection(remoteAddress: InetSocketAddress,
                                   inputStream: Producer[ByteString],
                                   outputStream: Consumer[ByteString]) {
    def handleWith(processor: Processor[ByteString, ByteString]): Unit = {
      processor.produceTo(outputStream)
      inputStream.produceTo(processor)
    }
  }

  object Connect {
    /**
     * Java API: factory method to create an Connect instance with default parameters
     */
    def create(settings: MaterializerSettings, remoteAddress: InetSocketAddress): Connect =
      apply(settings, remoteAddress)
  }

  case class Connect(settings: MaterializerSettings,
                     remoteAddress: InetSocketAddress,
                     localAddress: Option[InetSocketAddress] = None,
                     options: immutable.Traversable[SocketOption] = Nil,
                     timeout: Option[FiniteDuration] = None) {

    /**
     * Java API
     */
    def withLocalAddress(localAddress: InetSocketAddress): Connect =
      copy(localAddress = Option(localAddress))

    /**
     * Java API
     */
    def withSocketOptions(options: java.lang.Iterable[SocketOption]): Connect =
      copy(options = Util.immutableSeq(options))

    /**
     * Java API
     */
    def withTimeout(timeout: FiniteDuration): Connect =
      copy(timeout = Option(timeout))
  }

  object Bind {
    /**
     * Java API: factory method to create a Bind instance with default parameters
     */
    def create(settings: MaterializerSettings, localAddress: InetSocketAddress): Bind =
      apply(settings, localAddress)
  }

  case class Bind(settings: MaterializerSettings,
                  localAddress: InetSocketAddress,
                  backlog: Int = 100,
                  options: immutable.Traversable[SocketOption] = Nil) {

    /**
     * Java API
     */
    def withBacklog(backlog: Int): Bind = copy(backlog = backlog)

    /**
     * Java API
     */
    def withSocketOptions(options: java.lang.Iterable[SocketOption]): Bind =
      copy(options = Util.immutableSeq(options))

  }

}

/**
 * INTERNAL API
 */
private[akka] class StreamTcpExt(system: ExtendedActorSystem) extends IO.Extension {
  val manager: ActorRef = system.systemActorOf(Props[StreamTcpManager], name = "IO-TCP-STREAM")
}

/**
 * INTERNAL API
 */
private[akka] object StreamTcpManager {
  private[akka] case class ExposedProcessor(processor: Processor[ByteString, ByteString])
}

/**
 * INTERNAL API
 */
private[akka] class StreamTcpManager extends Actor {
  import StreamTcpManager._

  def receive: Receive = {
    case StreamTcp.Connect(settings, remoteAddress, localAddress, options, timeout) ⇒
      val processorActor = context.actorOf(TcpStreamActor.outboundProps(
        Tcp.Connect(remoteAddress, localAddress, options, timeout, pullMode = true),
        requester = sender(),
        settings))
      processorActor ! ExposedProcessor(new ActorProcessor[ByteString, ByteString](processorActor))

    case StreamTcp.Bind(settings, localAddress, backlog, options) ⇒
      val publisherActor = context.actorOf(TcpListenStreamActor.props(
        Tcp.Bind(context.system.deadLetters, localAddress, backlog, options, pullMode = true),
        requester = sender(),
        settings))
      publisherActor ! ExposedPublisher(new ActorPublisher(publisherActor))
  }

}

