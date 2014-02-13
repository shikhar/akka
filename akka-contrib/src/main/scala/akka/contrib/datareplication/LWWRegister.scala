/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.datareplication

object LWWRegister {
  def apply(initialValue: Any): LWWRegister =
    new LWWRegister(initialValue, defaultClock(), defaultClock)

  def unapply(value: Any): Option[Any] = value match {
    case r: LWWRegister ⇒ Some(r.value)
    case _              ⇒ None
  }

  /**
   * INTERNAL API
   */
  private[akka] abstract class Clock extends (() ⇒ Long) with Serializable
  private val defaultClock = new Clock {
    def apply() = System.currentTimeMillis()
  }
}

/**
 * Implements a 'Last Writer Wins Register' CDDT, also called a 'LWW-Register'.
 *
 * Merge takes the value of the register with highest timestamp. Note that this
 * relies on synchronized clocks. `LWWRegister` should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 */
case class LWWRegister(
  private[akka] val state: Any,
  private[akka] val timestamp: Long,
  private[akka] val clock: LWWRegister.Clock)
  extends ReplicatedData {

  type T = LWWRegister

  /**
   * Scala API
   */
  def value: Any = state

  /**
   * Scala API
   */
  def value_=(v: Any): LWWRegister =
    copy(state = v, timestamp = math.max(clock(), timestamp + 1))

  /**
   * Java API
   */
  def getValue(): AnyRef = state.asInstanceOf[AnyRef]

  /**
   * Java API
   */
  def setValue(v: AnyRef): LWWRegister =
    value = v

  override def merge(that: LWWRegister): LWWRegister =
    if (that.timestamp > this.timestamp) that
    else this
}

