package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.structures.Util;
import lombok.Data;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";
	public static boolean otherWorkerFoundSolution = false;
	public static int counterMessages = 0; //Hilfsvariable

	// Get all the constants from the Master
	private static final Character[] passwordCharsAsArray = Master.passwordCharsAsArray;
	public static final ArrayList<Character> PASSWORD_CHARS = Master.passwordChars;
	public static final Integer PASSWORD_LENGTH = Master.PASSWORD_LENGTH;

	private TreeMap<String, String> cache = new TreeMap<>();

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class HintMessage implements Serializable {
		private static final long serialVersionUID = 8343040842748609598L;
		public String hint;
		public Character symbolNotInUniverse;
		public ActorRef sender;
		public List<Map<Character, Character[][]>> permutationRanges;

		public HintMessage(String hint,
						   List<Map<Character, Character[][]>> permutationRanges,
						   Character symbolNotInUniverse,
						   ActorRef sender) {
			this.hint = hint;
			this.symbolNotInUniverse = symbolNotInUniverse;
			this.sender = sender;
			this.permutationRanges = permutationRanges;
		}
	}

	private Member masterSystem;
	private final Cluster cluster;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(HintMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(HintMessage message) {
		//otherWorkerFoundSolution = false;
		//System.out.println("MSG: " + message);

		String hint = message.hint;
		System.out.println("Worker " + this.self().path() + ": Received HintMessage with hint " + message.hint);
		boolean found = false;
		for (Map<Character, Character[][]> permutations : message.permutationRanges) {

			Character key = (Character) permutations.keySet().toArray()[0]; //the symbol of the permutation range
			//System.out.println("key: "+key);
			Character[] startPermutation = permutations.get(key)[0];
			Character[] endPermutation = permutations.get(key)[1];

			//System.out.println("WORKER " + this.self().path() + ": StartPermutation " + Arrays.toString(startPermutation));
			Character[] currentPermutation = startPermutation;

			/*
			TODO: momentan läuft nach der ersten Hint nurnoch Worker 1 und berechnet Kombinationen!
			--> Worker neu starten?
			 */
			//System.out.println("WORKER " + this.self().path() + ": CURRENT PERMUTATION: " + Arrays.toString(currentPermutation));
			while (!found && (!Arrays.equals(currentPermutation, endPermutation))) {
				if (otherWorkerFoundSolution) {
					//System.out.println("I won't work on this no more");
					break;
				}
				for (int i = 0; i < currentPermutation.length; i++) {
					String candidate = "";
					for (int j = 0; j < currentPermutation.length; j++) {
						candidate += currentPermutation[j];
					}
					//String hashedCandidate;
					/*
					if (this.cache.containsKey(candidate)) {
						hashedCandidate = this.cache.get(candidate);
					} else {
						hashedCandidate = this.hash(candidate);
						this.cache.put(candidate, hashedCandidate);
					}
					*/
					//if (counterMessages == 1) { //Debugging
					//	System.out.println("WORKER " + this.self().path() + ": " + candidate);
					//}
					String hashedCandidate = this.hash(candidate);
					if (hashedCandidate.equals(hint)) {
						found = true;
						otherWorkerFoundSolution = true; //TODO: in eine variable speichern
						//System.out.println("WORKER " + this.self().path() + ": HINT CRACKED: " + key + " CANDIDATE: " + candidate);

						HintMessage result = new HintMessage(hint, null, key, this.self());
						message.sender.tell(result, this.self());
						break;
					} else {
						//System.out.println("Current: "+Arrays.toString(currentPermutation));
						Util.findNextPermutation(currentPermutation);
					}
				}
			}
			if (found || otherWorkerFoundSolution) {
				//gehe nicht mehr durch die anderen character
				break;
			}
		}
		//System.out.println("WORKER " + self().path() + ": finished with task!");
	}

	//TODO: handle dafür, dass das Password universum korrigiert wurde

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;

			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
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
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}



	








}