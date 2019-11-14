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
    private static final int MAX_ALLOWED_STREAM_SIZE = 260000;
    private final StreamRefResolver streamRefResolver = StreamRefResolver.get(this.context().system());
    private final Materializer materializer = Materializer.apply(this.context().system());

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
                .match(MessageWrapper.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            // Call Kryo Serializer
            Output output = new Output(oos);
            Kryo kryo = new Kryo();
            kryo.writeClassAndObject(output, message.getMessage());

            // Need to call .close() before toByteArry to avoid “KryoException: Buffer underflow”
            output.flush();
            output.close();

            // message in Bytes
            byte[] byteArray = bos.toByteArray();
            this.log().info("Byte Array Length: " + byteArray.length);

            // Create chunks with ids for sort
            List<Node> noteList = new ArrayList<>();
            int counter = 1;
            for (int i = 0; i < byteArray.length; i += (MAX_ALLOWED_STREAM_SIZE + 1)) {
                int max = i + MAX_ALLOWED_STREAM_SIZE;
                if (i + MAX_ALLOWED_STREAM_SIZE >= byteArray.length)
                    max = byteArray.length - 1;
                noteList.add(new Node(counter++, Arrays.copyOfRange(byteArray, i, max + 1)));
            }

            // Akka Streams Source -> Graph -> Sink
            Source<Node, NotUsed> streamLogs = Source.from(noteList);
            SourceRef<Node> sourceRef = streamLogs.runWith(StreamRefs.sourceRef(), materializer);

            // SourceRef Wrapper
            receiverProxy.tell(new MessageWrapper(this.streamRefResolver.toSerializationFormat(sourceRef), this.sender(), receiver), this.self());
        } catch (IOException e) {
            this.log().error(e, e.getMessage());
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

    private void handle(MessageWrapper message) {
        ArrayList<Node> nodes = new ArrayList<>();
        this.streamRefResolver.resolveSourceRef(message.getSourceRef())
                .getSource()
                .runWith(Sink.foreach(e -> nodes.add((Node) e)), materializer)
                .thenAccept((f) -> {
                    // Sort Nodes for the correct order
                    nodes.sort(Node::compareTo);

                    // merging all byte arrays
                    byte[] result = {};
                    for (Node node : nodes) {
                        result = ArrayUtils.addAll(result, node.getByteArray());
                    }
                    this.log().info("Received Byte Array Length: " + result.length);

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(result);
                         ObjectInputStream ois = new ObjectInputStream(inputStream);
                         Input input = new Input(ois)) {

                        // deserialize
                        Kryo kryo = new Kryo();
                        message.getReceiver().tell(kryo.readClassAndObject(input), message.getSender());
                        this.log().info("Message send");
                    } catch (IOException e) {
                        this.log().error("Stream Error {}", e.getMessage());
                    } catch (Exception e) {
                        this.log().error(e, e.getMessage());
                    }
                });
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class MessageWrapper implements Serializable {
        private static final long serialVersionUID = 5707807743872319842L;
        private String sourceRef;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Node implements Serializable, Comparable<Node> {
        private static final long serialVersionUID = 4557807743872319842L;
        private int key;
        private byte[] byteArray;

        @Override
        public int compareTo(Node o) {
            return Integer.compare(this.key, o.getKey());
        }
    }
}
