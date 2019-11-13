package de.hpi.ddm.actors;

import java.io.*;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.*;

import akka.Done;
import akka.NotUsed;
import akka.actor.*;
import akka.event.LoggingAdapter;
import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.FiniteDuration;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;

		// added
		//private
		//private long uuid;
		//private short id;
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
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	/*
	public class Wrappper {

		private LargeMessageProxy getOuterClass() {
			return LargeMessageProxy.this;
		}

		private void tellRec(LargeMessage<?> msg, ActorSelection receiverProxy, ActorRef receiver) {
			//ActorSelection receiverProxy = getOuterClass().context().actorSelection(receiver.path().child(DEFAULT_NAME));
			receiverProxy.tell(new BytesMessage(msg.getMessage(),getOuterClass().sender(),msg.getReceiver()),getOuterClass().self());
		}

		private CompletionStage<Done> sendStuff(LargeMessage<?> msg, ActorSelection receiverProxy, ActorRef receiver) {
			return Source.single(msg).runWith(tellRec(msg, receiverProxy, receiver), ActorMaterializer.create(getOuterClass().context()));
		}
	}

	private Flow<LargeMessage<?>, BytesMessage<?>, NotUsed> sendMessage(LargeMessage<?> msg) {
		ActorRef sender = this.getSender();
		ActorRef receiver = msg.getReceiver();
		//return

		//return Flow.of(LargeMessage.class).flatMapConcat(msg -> {

		//});
	}

	private Flow<LargeMessage<?>, BytesMessage<?>, NotUsed> toBytes() {
		return Flow.of(LargeMessage.class).
	}

	private Sink<LargeMessage<?>, CompletionStage<Done>> getMsg() {
		return Flow.of(LargeMessage.class).via(toBytes());
	}

	private CompletionStage<Done> sendStuff(){
		private CompletionStage
	}
	*/

	/*
	private Byte[] serializeMessage(LargeMessage<?> msg, Output output){

		Kryo kryo = new Kryo();
		//kryo.writeClassAndObject(output,msg);
		//ObjectOutputStream stream = new ObjectOutputStream(output);
		ByteArrayOutputStream outstream = new ByteArrayOutputStream();
		Output out = new Output(outstream);
		kryo.writeClassAndObject(out,msg);
		outstream.close();
		out.close();
		return outstream.toByteArray();

	}
	*/

	public static byte[] objToByte(LargeMessage<?> msg) throws IOException {
		//ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		//ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
		//objStream.writeObject(msg);

		//return byteStream.toByteArray();
		try {
			Kryo k = new Kryo();
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Output output = new Output(out);
			k.writeClassAndObject(output,msg);
			output.close();
			out.close();
			return out.toByteArray();

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}


	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream stream = null; // check if init necessary


		// define how many blocks the message consists of
		// hardcoded -> bad solution!!
		int block_size = 4096; // 4KB

		// generate UUID
		UUID uuid = UUID.randomUUID();
		long message_id = uuid.getMostSignificantBits();





		try {
			stream = new ObjectOutputStream(outputStream);
			stream.writeObject(message);
			stream.flush();

			byte[] bytes = outputStream.toByteArray();
			int byte_length = bytes.length;

			//System.out.println(bytes);

			int block_pointer = 0;
			boolean last = false;

			short block_id = 0;

			while (true) {
				BytesMessage<byte[]> block = new BytesMessage<>();


				if (block_pointer+block_size>byte_length) {
					block.bytes = Arrays.copyOfRange(bytes,block_pointer,byte_length);
					last = true;
				} else {
					block.bytes = Arrays.copyOfRange(bytes, block_pointer, block_pointer + block_size);
				}

				block.uuid = message_id;
				block.sender = this.sender();
				block.receiver = receiver;
				block.id = block_id;

				// tell receiver
				receiverProxy.tell(block,this.self());
				if (last) {
					break;
				}
				// update block_pointer
				block_pointer = block_pointer + block_size;

			}

		} catch (IOException e) {
			System.out.println("IOException occured!");
		}



		/*
		System.out.println("NOW TESTING");
		try {
			System.out.println(objToByte(message));
		} catch (IOException e) {

			e.printStackTrace();
		}
		*/
		//message
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		//BytesMessage<?> msg = new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver());
		//System.out.println("TESTOUT: "+msg.bytes);

		// Use Akka Streaming
		//Source<LargeMessage<?>, NotUsed> src = Source.single(message);
		//Flow<LargeMessage<?>, BytesMessage<?>, NotUsed> flow = new Flow<LargeMessage<?>, BytesMessage<?>, NotUsed>();
		//Source.single(message).runWith(getMsg(), ActorMaterializer.create(this.context()));
		//src.runForeach(i -> receiverProxy.tell(new BytesMessage<>(i.getMessage(), this.sender(),i.getReceiver()),this.self()));
		//Object




		//receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		//Sink<BytesMessage<?>, NotUsed> snk = Sink
		//message.getReceiver().tell(message.getBytes(), message.getSender());

		ByteArrayInputStream in = new ByteArrayInputStream((byte[])message.bytes);
		ObjectInputStream in_stream = new ObjectInputStream(in);
		System.out.println(in);




		byte[] msg = (byte[]) message.bytes;
		System.out.println("HIER SCHAUEN: "+msg);
	}
}
