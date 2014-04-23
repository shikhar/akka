package akka.stream.javadsl;

import java.util.Arrays;
import java.util.Collections;

import org.junit.ClassRule;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Function;
import akka.japi.Function2;
import akka.japi.Procedure;
import akka.stream.FlowMaterializer;
import akka.stream.MaterializerSettings;
import akka.stream.testkit.AkkaSpec;
import akka.testkit.TestProbe;

public class FlowTest {

  @ClassRule
  public static AkkaJUnitActorSystemResource actorSystemResource = new AkkaJUnitActorSystemResource("StashJavaAPI",
      AkkaSpec.testConf());

  final ActorSystem system = actorSystemResource.getSystem();

  final MaterializerSettings settings = MaterializerSettings.create();
  final FlowMaterializer materializer = FlowMaterializer.create(settings, system);

  @Test
  public void mustBeAbleToUseSimpleOperators() {
    final TestProbe probe = new TestProbe(system);
    final String[] lookup = { "a", "b", "c", "d", "e", "f" };
    final java.lang.Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5);
    Flow.create(input).drop(2).take(3).map(new Function<Integer, String>() {
      public String apply(Integer elem) {
        return lookup[elem];
      }
    }).filter(new Predicate<String>() {
      public boolean defined(String elem) {
        return !elem.equals("c");
      }
    }).grouped(2).mapConcat(new Function<java.util.List<String>, java.util.List<String>>() {
      public java.util.List<String> apply(java.util.List<String> elem) {
        return elem;
      }
    }).fold("", new Function2<String, String, String>() {
      public String apply(String acc, String elem) {
        return acc + elem;
      }
    }).foreach(new Procedure<String>() {
      public void apply(String elem) {
        probe.ref().tell(elem, ActorRef.noSender());
      }
    }).consume(materializer);

    probe.expectMsg("de");

  }

  @Test
  public void mustBeAbleToUseTransform() {
    final TestProbe probe = new TestProbe(system);
    final TestProbe probe2 = new TestProbe(system);
    final java.lang.Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);
    // duplicate each element, stop after 4 elements, and emit sum to the end
    Flow.create(input).transform(new Transformer<Integer, Integer>() {
      int sum = 0;
      int count = 0;

      @Override
      public Iterable<Integer> onNext(Integer element) {
        sum += element;
        count += 1;
        return Arrays.asList(element, element);
      }

      @Override
      public boolean isComplete() {
        return count == 4;
      }

      @Override
      public Iterable<Integer> onComplete() {
        return Collections.singletonList(sum);
      }

      @Override
      public void cleanup() {
        probe2.ref().tell("cleanup", ActorRef.noSender());
      }
    }).foreach(new Procedure<Integer>() {
      public void apply(Integer elem) {
        probe.ref().tell(elem, ActorRef.noSender());
      }
    }).consume(materializer);

    probe.expectMsg(0);
    probe.expectMsg(0);
    probe.expectMsg(1);
    probe.expectMsg(1);
    probe.expectMsg(2);
    probe.expectMsg(2);
    probe.expectMsg(3);
    probe.expectMsg(3);
    probe.expectMsg(6);
    probe2.expectMsg("cleanup");
  }

  @Test
  public void mustBeAbleToUseTransformRecover() {
    final TestProbe probe = new TestProbe(system);
    final java.lang.Iterable<Integer> input = Arrays.asList(0, 1, 2, 3, 4, 5);
    Flow.create(input).map(new Function<Integer, Integer>() {
      public Integer apply(Integer elem) {
        if (elem == 4)
          throw new IllegalArgumentException("4 not allowed");
        else
          return elem + elem;
      }
    }).transformRecover(new RecoveryTransformer<Integer, String>() {

      @Override
      public Iterable<String> onNext(Integer element) {
        return Collections.singletonList(element.toString());
      }

      @Override
      public Iterable<String> onError(Throwable e) {
        return Collections.singletonList(e.getMessage());
      }

      @Override
      public boolean isComplete() {
        return false;
      }

      @Override
      public Iterable<String> onComplete() {
        return Collections.emptyList();
      }

      @Override
      public void cleanup() {
      }

    }).foreach(new Procedure<String>() {
      public void apply(String elem) {
        probe.ref().tell(elem, ActorRef.noSender());
      }
    }).consume(materializer);

    probe.expectMsg("0");
    probe.expectMsg("2");
    probe.expectMsg("4");
    probe.expectMsg("6");
    probe.expectMsg("4 not allowed");
  }

  // FIXME add tests for all remaining operators
}
