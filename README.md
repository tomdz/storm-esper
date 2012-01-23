# Storm-Esper

A library to integrate [Esper](http://esper.codehaus.org) with [Storm](https://github.com/nathanmarz/storm).

## License

Esper is GPLv2 licensed, which means that this library is GPLv2 licensed, as well. You can find the license [here](http://www.opensource.org/licenses/gpl-2.0.php).

## Description

The `storm-esper` library provides a [bolt](https://github.com/nathanmarz/storm/wiki/Concepts) that allows you to use Esper queries on Storm data streams.

The bolt is created via a builder:

        EsperBolt esperBolt =
            new EsperBolt.Builder()
                         .addInputAlias("some-spout", "default", "Test")
                         .setAnonymousOutput("default", "min", "max")
                         .addStatement("select max(a) as max, min(b) as min from Test.win:length_batch(4)")
                         .build();

An input alias defines an alias for a Storm stream that can be used in the Esper queries. By default, the bolt will assign the event name `<component id>_<stream id>` where `component id` is the name of the spout/bolt that generates the data to be fed into the Esper bolt, and `stream id` being the id of the specific stream that the Esper bolt is connected to (if the source spout/bolt only has one output, then this is usually `default`). The method `addInputAlias` allows to define a different name for a specific stream of a specific component, e.g. in the example `Stream` for the stream `default` of component `some-spout`.

By default, the bolt defines a single output stream which corresponds to the unnamed stream that basic `SELECT` esper statements select values into. In order to tell Storm about the generated tuples, you need to declare the fields of this stream via the `setAnonymousOutput` method. It takes the target stream id to use (usually `default`) and the names of the fields which are the union of the output tuples of all Esper statements. E.g. if you have two `SELECT` statements that select `a` and `b` respectively, then you'd call `setAnonymousOutput("default", "a", "b")`.

If the Esper statements generate names streams (e.g. `insert into <name>`), then you can use the `addNamedOutput(<stream id>, <name>, <field 1>, <field 2>, ...)` method instead to map named Esper streams to Storm streams. This allows you to generate more than stream from one Esper bolt.

Finally, you add the individual Esper statements via the `addStatement` method. Note that it does not support adding more than one Esper statement at the same time - use multiple `addStatement` calls for this.

## How to get

Use a Maven dependency in your pom.xml:

    <dependency>
      <groupId>org.tomdz.storm</groupId>
      <artifactId>storm-esper</artifactId>
      <version>0.6.2-SNAPSHOT</version>
    </dependency>
