package de.hpi.ddm.actors;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import akka.actor.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////


	private Map<String, LargeMessage<?>> messages = new HashMap<>();


	//private List<BytesMessage<?>> messages = new ArrayList<>();

	public static final String DEFAULT_NAME = "largeMessageProxy";
	// define how many blocks the message consists of
	// hardcoded -> bad solution!!
	public static final short BLOCK_SIZE = 2048; // 2KB



	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;

		// added
		private long uuid;
		private short id;
		private short max_id = 0;
		private short length;
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
			k.writeClassAndObject(output, msg);
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

		UUID uuid = UUID.randomUUID();
		long message_id = uuid.getMostSignificantBits();

		try {
			stream = new ObjectOutputStream(outputStream);
			stream.writeObject(message);
			stream.flush();

			// Message
			byte[] bytes = outputStream.toByteArray();
			System.out.println(bytesToHex(bytes));

			int byte_length = bytes.length;

			// max id = amount of chunks-1
			System.out.println("Byte_length: " + byte_length);
			System.out.println("block_size: " + BLOCK_SIZE);
			short max_id = (short) Math.ceil(byte_length / BLOCK_SIZE);

			// current block pointer (bytes)
			int block_pointer = 0;
			boolean last = false;

			// current block id
			short block_id = 0;

			while (true) {
				//stream.flush();
				BytesMessage<byte[]> block = new BytesMessage<>();

				if (block_pointer + BLOCK_SIZE > byte_length) {
					block.bytes = Arrays.copyOfRange(bytes, block_pointer, byte_length);
					// TODO: cast is bad
					block.length = (short) (byte_length - block_pointer);
					last = true;
				} else {
					block.bytes = Arrays.copyOfRange(bytes, block_pointer, block_pointer + BLOCK_SIZE);
					block.length = BLOCK_SIZE;
				}
				/*
				block.uuid = message_id;
				System.out.println("UUID: "+block.uuid);
				block.sender = this.sender();
				System.out.println("SENDER: "+block.sender);
				block.receiver = receiver;
				System.out.println("RECEIVER: "+block.receiver);
				block.id = block_id;
				System.out.println("BLOCKID: "+block.id);
				block.max_id = max_id;
				System.out.println("MAX_ID: "+block.max_id);
				System.out.println("LENGTH: "+block.length);
				*/
				// tell receiver
				receiverProxy.tell(block, this.self());
				if (last) {
					break;
				}
				// update block_pointer
				block_pointer = block_pointer + BLOCK_SIZE;
				block_id++;

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


	Map<Long, Map<Short, byte[]>> map = new HashMap<>();
	int array_size = 0;


	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		//Sink<BytesMessage<?>, NotUsed> snk = Sink

		// write uuid to Map
		if (!map.containsKey(message.uuid)) {
			map.put(message.uuid, new HashMap<>());
		}

		// to inner map
		Map<Short, byte[]> inner_map = map.get(message.uuid);
		inner_map.put(message.id, (byte[]) message.bytes);
		array_size = array_size + message.length;

		//for (Map.Entry<Short, byte[]> entry : inner_map.entrySet()) {
		//	System.out.println(entry.getKey() + ":" + entry.getValue().toString());

		//}

		//System.out.println("Max_ID ist: "+message.max_id);

		List<Short> innermap_as_list = inner_map.keySet().stream().sorted().collect(Collectors.toList());


		// print all keys of inner list (all message chunks, belonging to one single message)
		//innermap_as_list.forEach((i)->System.out.println(i));


		// if all message chunks received...
		// == if received chunk has same id as max_id
		if (message.max_id == message.id) {

			// all chunks are received
			/*
			inner_map.entrySet().forEach(entry->{
				System.out.println(entry.getKey() + " " + entry.getValue());
			});
			*/




			// right now it is required that last received byte is last byte of message
			// OK - complete_message has correct size
			byte[] complete_message = new byte[array_size];
			//System.out.println("TEST_HERE: "+complete_message.length);

			for (Short i : innermap_as_list) {
				//Short index = innermap_as_list.get(i);
				// block_size = 2048 -> TODO: hardcoded right now
				/*
				System.out.println("########################");
				System.out.println("DEBUG: "+inner_map.get(index));
				System.out.println("DEBUG: "+complete_message);
				System.out.println("DEBUG: "+index*2048);
				System.out.println("DEBUG: "+inner_map.get(index).length);
				System.out.println("########################");
				*/
				//System.arraycopy();
				// hier weiter
				System.arraycopy(inner_map.get(i),0, complete_message,i*BLOCK_SIZE, inner_map.get(i).length);
			}
			System.out.println(bytesToHex(complete_message));
			// view content of complete message
			//for (int i = 0; i<array_size;i++){
				//int positive = complete_message[i] & 0xff;

			//}


			ByteArrayInputStream bytes_in = new ByteArrayInputStream(complete_message);
			try {
				ObjectInputStream object_in = new ObjectInputStream(bytes_in);

				Object o = object_in.readObject();

				Object s = KryoPoolSingleton.get().fromBytes(complete_message);

				System.out.println(s);
				message.getReceiver().tell(s, message.getSender());
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}






		// inner_map.keySet().forEach( (value) -> System.out.println(value));



		/*
		short max_id = message.max_id;

		ByteArrayInputStream in = new ByteArrayInputStream((byte[]) message.bytes);

		try {
			ObjectInputStream in_stream = new ObjectInputStream(in);
			LargeMessage<?> orig = (LargeMessage<?>) in_stream.readObject();
			//messages
			messages.put(""+message.uuid+"_"+message.id, orig);
			message.getReceiver().tell(message.getBytes(), message.getSender());

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		//System.out.println(in);





		if (max_id == this.messages.size()) {
			// extrahiere alle schlüssel
			String[] keys = new String[messages.size()];
			messages.keySet().toArray(keys);

			// iteriere durch die keys
			for (int i = 0; i<max_id; i++) {
				if (keys[i].startsWith(""+message.uuid)) {
					String id_part = keys[i].split("_")[1];
					System.out.println(id_part);
				}
			}

			//iteriere durch die hashmap
			//for (int i = 0; i<max_id; i++) {

			//}
			//baue schlüssel auseinander
			//behalte nur die mit der uuid von oben

			//sortiere nach der id
			//baue die messages zusammen

		}

		byte[] msg = (byte[]) message.bytes;
		System.out.println("HIER SCHAUEN: "+msg);
	}
	*/

	}
}
