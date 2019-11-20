package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.*;
import de.hpi.ddm.structures.Util;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static final Character[] passwordCharsAsArray = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'};
	public static ArrayList<Character> passwordChars = null;
	public static final Integer PASSWORD_LENGTH = 10;

	private List<String[]> passwordFile = new ArrayList<>();
	private Integer currentLineInList = 0;
	private Integer currentColumnInList = 5;
	private String hintReceived = "";
	private List<String> combinationsOfPasswords = new ArrayList<>();

	Map<Integer, List<Map<Character,Character[][]>>> mainMap = new HashMap<>();

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
				.match(Worker.HintMessage.class, this::handle)
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

	protected void handle(Worker.HintMessage message) {
		// a worker has successfully cracked a hint -> TODO: log it
		this.log().info("Worker " + message.sender.path() + ": " + message.hint + " cracked and sent back to master");
		if (hintReceived.equals("") || !hintReceived.equals(message.hint)) {
			hintReceived=message.hint;
			currentColumnInList++;
			this.passwordChars.remove(message.symbolNotInUniverse);
			//System.out.println("Column inc: "+currentColumnInList);
		}
		// System.out.println("MSG: "+message);
		// HintMessage back to master -> jump to next line
		if (passwordFile.get(0).length==currentColumnInList) {
			//TODO: brute force pw

			//System.out.println("PASSWORD UNIVERSE AFTER HINT-CRACKING: " + Arrays.toString(this.passwordChars.toArray()));
			String password = this.bruteForce(this.passwordFile.get(currentLineInList)[4],
												Integer.parseInt(this.passwordFile.get(currentLineInList)[3]));
			System.out.println("MASTER FOUND PASSWORD: "+ password);
			currentColumnInList = 5;
			currentLineInList++;
			this.passwordChars = new ArrayList(Arrays.asList(passwordCharsAsArray));
		}
		if (passwordFile.size()-1==currentLineInList){
			System.out.println("finish");
		}

		ActorRef worker = message.sender;
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Worker.otherWorkerFoundSolution = false;
		Worker.counterMessages++; // Hilfsvariable für Debugging

		/*
		Integer i = 0;
		Pattern p = Pattern.compile("[0-9]+$");
		Matcher m = p.matcher(worker.path().toString());
		if(m.find()) {
			i = Integer.parseInt(m.group());

		}
		*/
		// System.out.println("Sending new message to worker " + i);
		for (int i = 0; i < this.workers.size(); i++) {
			Worker.HintMessage msg = new Worker.HintMessage(this.passwordFile.get(currentLineInList)[currentColumnInList],
					this.mainMap.get(i),null, this.self());
			this.workers.get(i).tell(msg,this.self());
		}


		// stop all the other workers - no need to work at the current hint anymore
	}

	protected void handle(BatchMessage message) {

		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		this.passwordChars = new ArrayList(Arrays.asList(passwordCharsAsArray));
		if (passwordFile.isEmpty()){
			passwordFile = new ArrayList<>(message.lines);
		}

		List<Integer> startIndices = new ArrayList<>();
		List<Integer> endIndices = new ArrayList<>();

		List<Character[]> startPermutations = new ArrayList<>();
		List<Character[]> endPermutations = new ArrayList<>();


		for (int i = 0; i < this.workers.size(); i++) {
			mainMap.put(i, new ArrayList<>());
		}

		// TODO: generisch machen
		Character[] curPermutation = {'A','B','C','D','E','F','G','H','I','J','K'};
		Character[] subPermutation;

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}


		String alphabet = message.lines.get(currentLineInList)[2];
		int alphabetSize = alphabet.length();

		int amountPermutations = f(alphabetSize - 1); // um eins reduziert, da subkandidaten betrachtet werden

		double amountPerWorker = amountPermutations * 1.0 / this.workers.size();

		int intPart = (int) amountPerWorker;

		double decimalDigits = amountPerWorker - intPart;
		// extraPermutations is always < this.workers.size() (amount of workers)
		// distribute extraPermutations to workers !!!
		int extraPermutations = (int) Math.ceil(decimalDigits * this.workers.size());

		int pointer = 0;

		for (int i = 0; i < this.workers.size(); i++) {
			int amountToCalculate = (int) amountPerWorker;
			if (extraPermutations > 0) {
				amountToCalculate++;
				extraPermutations--;
			}

			int start = pointer;

			int end = pointer + amountToCalculate - 1;
			pointer = pointer + amountToCalculate;

			startIndices.add(start);
			endIndices.add(end);
		}

		int workerId = 0;
		Character[][] permutationRange = new Character[2][alphabetSize - 1]; //TODO: stimmt -1?

		// {A, B, C, D, E, F, G, H, I, J, K}
		for (Character key : curPermutation) {
			subPermutation = Arrays.stream(curPermutation).filter(value -> value != key).toArray(Character[]::new);
			// System.out.println("SubPermutation: " + Arrays.toString(subPermutation));

			for (int i = 0; i < amountPermutations; i++) {
				Util.findNextPermutation(subPermutation);

				if (startIndices.contains(i)) {

					permutationRange[0] = Arrays.copyOf(subPermutation, subPermutation.length);
				}
				if (endIndices.contains(i)) {

					permutationRange[1] = Arrays.copyOf(subPermutation, subPermutation.length);
					//System.out.println("permutationRange: "+Arrays.toString(permutationRange));
					Character[][] copyPermutationRange = Arrays.copyOf(permutationRange, permutationRange.length);
					//System.out.println(Arrays.toString(copyPermutationRange));
					// Bsp: In Map<A,_> finden sich die Start- und Endpermutationen OHNE den char A
					Map<Character, Character[][]> innerMap = new HashMap<>();

					//wenn zugriff nicht null ist hol dir die werte, speichere die in der liste, füge der liste den neuen
					//wert hinzu und speichere die vereinigung in der map ab

					innerMap.put(key, copyPermutationRange);

					List<Map<Character, Character[][]>> innerMaps;
					innerMaps = mainMap.get(workerId);
					innerMaps.add(innerMap);

					mainMap.put(workerId, innerMaps);
					// this only works for the first row (get 0) and first hint (column 5)
					workerId++;
					//endPermutations.add(curPermutation);
				}
			}
			workerId = 0;
		}

		System.out.println("MainMap: "+mainMap.toString());
		for (int i = 0; i < this.workers.size(); i++) {
			this.workers.get(i).tell(
					new Worker.HintMessage(
							message.lines.get(currentLineInList)[currentColumnInList],
							mainMap.get(i),
							null,
							this.self()),
					this.self());
		}
	}
	/*
	public String convertWithIteration(Map<ActorPath, Character[][]> map) {
		String mapAsString = "";
		String values = "";
		Character[] firstPermutation;
		Character[] secondPermutation;
		for (ActorPath key : map.keySet()) {
			firstPermutation = map.get(key)[0];
			secondPermutation = map.get(key)[1];
			values = Arrays.toString(firstPermutation) + " | " + Arrays.toString(secondPermutation);
			for (int i=0; i<firstPermutation.length; i++){
				System.out.print(firstPermutation[i]+" ");
			}
			System.out.println();
			for (int j=0; j<secondPermutation.length; j++){
				System.out.print(secondPermutation[j]+" ");
			}
			System.out.println();

			mapAsString += (key + ": " + values);
		}
		return mapAsString;
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

	private String bruteForce(String hashedPassword, int length) {
		String solution = "";
		this.combinations(this.passwordChars, length);
		for (String candidate: this.combinationsOfPasswords) {
			//System.out.println(candidate);
			String hashedCandidate = this.hash(candidate);
			//System.out.println(hashedCandidate + " | " + hashedPassword);
			if (hashedCandidate.equals(hashedPassword)) {
				System.out.println("LÄSUNG GEFUNDÄNG");
				solution = candidate;
				this.combinationsOfPasswords = new ArrayList<>();
				break;
			}
		}
		return solution;
	}

	private void combinations(List<Character> chars, int length) {
		Character[] universe =  chars.toArray(new Character[chars.size()]);
		List<Character> combinations = new ArrayList<>();
		recur(universe, combinations, length, 0, universe.length);
	}

	// TODO: rausschmeißen, doppelter Code mit Worker hash
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}




	public void recur(Character[] A, List<Character> out,
							 int k, int i, int n)
	{
		// base case: if combination size is k, print it
		if (out.size() == k)
		{
			String str = out.stream().map(e->e.toString()).reduce((acc, e) -> acc  + e).get();
			this.combinationsOfPasswords.add(str);
			return;
		}

		// start from previous element in the current combination
		// till last element
		for (int j = 0; j < n; j++)
		{
			// add current element A[j] to the solution and recur with
			// same index j (as repeated elements are allowed in combinations)
			out.add(A[j]);
			recur(A, out, k, j, n);

			// backtrack - remove current element from solution
			out.remove(out.size() - 1);
		}
	}







}
