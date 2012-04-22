# Storm-Esper

A library to integrate [Esper](http://esper.codehaus.org) with [Storm](https://github.com/nathanmarz/storm).

## License

Esper is GPLv2 licensed, which means that this library is GPLv2 licensed, as well. You can find the license
[here](http://www.opensource.org/licenses/gpl-2.0.php).

## Description

The `storm-esper` library provides a [bolt](https://github.com/nathanmarz/storm/wiki/Concepts) that allows
you to use Esper queries on Storm data streams.

The bolt is created via a builder:

    EsperBolt esperBolt =
        new EsperBolt.Builder()
                     .inputs()
                        .aliasComponent("some-spout")
                        .withFields("a", "b")
                        .ofType(Integer.class)
                        .toEventType("Test")
                     .outputs()
                        .outputs().onDefaultStream().emit("min", "max")
                     .statements()
                        .add("select max(a) as max, min(b) as min from Test.win:length_batch(4)")
                     .build();

### `inputs`

The `inputs` section allows you to define aliases for storm streams and type hints. An input alias defines an
alias for a Storm stream that can be used in the Esper queries instead of the automatically generated one.
By default, the bolt will assign the event name `<component id>_<stream id>` where `component id` is the name
of the spout/bolt that generates the data to be fed into the Esper bolt, and `stream id` is the id of the
specific stream that the Esper bolt is connected to (if the source spout/bolt only has one output, then this
is usually `default`). The `aliasComponent` and `aliasStream` methods allows to define a different name for a
specific stream of a specific component (`default` in the case of `aliasComponent`). E.g. in the example
the stream `default` of the component `some-spout` was mapped to the Esper event type `Test`.
Type hints are necessary to give Esper enough information about how to deal with properties of event types.
Storm streams are untyped in the sense that Storm itself does not maintain type information for the individual
fields in a tuple. On the other hand, Esper requires strongly typed event type definitions. By default,
`storm-esper` will use `Object` for all properties which works fine for counting, `max` and `min`, etc. However
for Esper functions that require numbers (such as `sum`) this does not work and you'll need to define a type
hint. In the example above, fields `a` and `b` of the tuples coming from `some-spout` are declared to be of
type `Integer` (which is equivalent to `int` as far as Esper is concerned).

### `outputs`

In order to push data from Esper back into Storm, you'll need to specify which streams the bolt will emit
(via `onDefaultStream` for the default stream which is called `default`, or via `onStream` for named streams),
and also which properties from which event types to emit into these streams (`emit` method).
Basic `SELECT` esper statements (as in the example above) work on a default anonymous event type. Unless you
tell the builder to use a specific event type (with `fromEventType`), it will assume this default event type.

### `statements`

This is basically the list of statements to give to Esper. If the Esper statements generate new events
 (e.g. `insert into <name>`), then you can use select these via the `fromEventType` method in the builder
in order to map them to Storm streams. This allows you to generate more than stream from one Esper bolt.

## How to get

Use a Maven dependency in your pom.xml:

    <dependency>
      <groupId>org.tomdz.storm</groupId>
      <artifactId>storm-esper</artifactId>
      <version>0.7.1-SNAPSHOT</version>
    </dependency>
