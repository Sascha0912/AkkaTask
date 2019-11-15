package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.StreamRefResolver;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";
    
    
    private final StreamRefResolver stream_reference_resolver= StreamRefResolver.get(this.context().system());
    private final Materializer mat = Materializer.apply(this.context().system());


    private static final int STREAM_SIZE = 260000; // 26kB
    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(Wrapper.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        try (
            // create a ByteArrayStream -> required for ObjectOutputStream
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            // ObjectOutputStream receives a reference to the ByteArrayOutput Stream
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {



            // Create a new Output based on the ObjectOutputStream
            Output output = new Output(objectOutputStream);

            // Create a new Kryo Object to serialize the message and write it to the Output
            Kryo kryo = new Kryo();
            kryo.writeClassAndObject(output, message.getMessage());
            output.flush();
            output.close();

            // message in Bytes
            byte[] message_in_bytes = byteArrayOutputStream.toByteArray();
            System.out.println(message_in_bytes.length);
            System.out.println(STREAM_SIZE);


            // Generate an empty list to hold Chunks
            List<Chunk> chunks = new ArrayList<>();
            // Create Counter variable
            int cnt = 1;


            for (int i = 0; i < message_in_bytes.length; i += (STREAM_SIZE + 1)) {


                int max = i + STREAM_SIZE;

                if (i + STREAM_SIZE >= message_in_bytes.length) {
                    max = message_in_bytes.length - 1;
                }

                // add new Chunk to the list of chunks
                chunks.add(new Chunk(cnt++, Arrays.copyOfRange(message_in_bytes, i, max + 1)));

            }

            // Using the Akka Streaming Module to build a Source Object out of the list of chunks
            Source<Chunk, NotUsed> streamLogs = Source.from(chunks);
            SourceRef<Chunk> sourceRef = streamLogs.runWith(StreamRefs.sourceRef(), mat);

            // tell the receiver proxy
            receiverProxy.tell(new Wrapper(this.stream_reference_resolver.toSerializationFormat(sourceRef), this.sender(), receiver), this.self());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /////////////////
    // Actor State //
    /////////////////

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void handle(Wrapper message) {
        ArrayList<Chunk> nodes = new ArrayList<>();
        this.stream_reference_resolver.resolveSourceRef(message.getSourceRef())
                .getSource()
                .runWith(Sink.foreach(e -> nodes.add((Chunk) e)), mat)
                .thenAccept((f) -> {
                    // Sort Nodes for the correct order
                    nodes.sort(Chunk::compareTo);

                    // combine all the arrays
                    byte[] result = {};
                    for (Chunk node : nodes) {
                        result = ArrayUtils.addAll(result, node.getByteArray());
                    }

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(result);
                         ObjectInputStream ois = new ObjectInputStream(inputStream);
                         Input input = new Input(ois)) {

                        // deserialize
                        Kryo kryo = new Kryo();
                        message.getReceiver().tell(kryo.readClassAndObject(input), message.getSender());
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 8193997184620822218L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Wrapper implements Serializable {
        private static final long serialVersionUID = -8180912932060922349L;
        private String sourceRef;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Chunk implements Serializable, Comparable<Chunk> {
        private static final long serialVersionUID = -1172402643941689635L;
        private int key;
        private byte[] byteArray;

        @Override
        public int compareTo(Chunk o) {
            return Integer.compare(this.key, o.getKey());
        }
    }
}
