package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.Util;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////
	/*
	@Data
	public static class HintMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private String hint;
		private char symbolNotInUniverse;
		private ActorRef sender;

		public HintMessage(String hint, char symbolNotInUniverse, ActorRef sender) {
			this.hint = hint;
			this.symbolNotInUniverse = symbolNotInUniverse;
			this.sender = sender;
		}
	}
	*/
	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	public static int f( final int n )
	{
		return n == 0 ? 1 : n * f( n - 1 );
	}

	protected void handle(BatchMessage message) {

		List<Integer> startIndices = new ArrayList<>();
		List<Integer> endIndices = new ArrayList<>();

		List<Character[]> startPermutations = new ArrayList<>();
		List<Character[]> endPermutations = new ArrayList<>();

		//Map<String,Integer[]> startEnd = new HashMap();

		Character[] curPermutation = {'A','B','C','D','E','F','G','H','I','J','K'};

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}


			// Worker.HintMessage msg = new Worker.HintMessage(lineAsArray[5],' ',this.self());
			String alphabet = message.lines.get(0)[2];
			int alphabetSize = 3; //alphabet.length();
			//System.out.println("AlphabetSize: "+ alphabetSize);

			int amountPermutations = f(alphabetSize);
			//System.out.println("AmountPermutations: "+amountPermutations);
			double amountPerWorker = amountPermutations * 1.0 / this.workers.size();
			//System.out.println("AmountPerWorker: "+amountPerWorker);
			int intPart = (int) amountPerWorker;
			//System.out.println("IntPart: "+intPart);
			double decimalDigits = amountPerWorker-intPart;
			//System.out.println("DecimalDigits: "+decimalDigits);
			// extraPermutations is always < this.workers.size() (amount of workers)
			// distribute extraPermutations to workers !!!
			int extraPermutations = (int) Math.ceil(decimalDigits * this.workers.size());
			//System.out.println("ExtraPermutations: "+extraPermutations);

			int pointer = 0;

			// Character[] chars = alphabet

			for (int i=0; i < this.workers.size(); i++){
				int amountToCalculate = (int) amountPerWorker;
				if (extraPermutations>0){
					amountToCalculate++;
					extraPermutations--;
				}

				int start = pointer;
				int end = pointer + amountToCalculate-1;
				pointer = pointer + amountToCalculate;
				//System.out.println("Start: "+start);
				//System.out.println("End: "+ end);

				startIndices.add(start);
				endIndices.add(end);

				Integer[][] startPlusEnd = {{start},{end}};
				//startEnd.put("workerID)

			}

			for (int i=0; i<amountPermutations;i++){
				Util.findNextPermutation(curPermutation);
				if (startIndices.contains(i)){
					startPermutations.add(curPermutation);
				}
				if (endIndices.contains(i)){
					endPermutations.add(curPermutation);
				}
			}
			System.out.println("StartPermutations: "+startPermutations);
			System.out.println("EndPermutations: "+endPermutations);





	}




	/*

	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		//System.out.println("Das sind alle: "+this.workers);

		//System.out.println(message.lines);
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		message.lines.forEach(lineAsArray -> {
			int worker_id = 0;
			//System.out.println(lineAsArray[4]); //password
			for (int hint_id = 5; hint_id < lineAsArray.length; hint_id++){
				// TODO: find better solution
				Worker.HintMessage hi  = new Worker.HintMessage(lineAsArray[hint_id],' ',this.self());

				this.workers.get(worker_id++).tell(hi,this.self());
				if (worker_id == this.workers.size()) {
					//if all workers have received a hint, get back to the first worker
					worker_id = 0;
				}
			}
		});
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	*/


	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
	this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
 	this.log().info("Unregistered {}", message.getActor());
	}
}
