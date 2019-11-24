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
		System.out.println("WORKER " + this.self().path() + ": RECEIVED HINT " + message.hint);
		// Print out the permutation ranges
		/*
		for (int i = 0; i < message.permutationRanges.size(); i++){
			//List<Map<Character, Character[][]>> li = new ArrayList<>();
			for (Map.Entry<Character, Character[][]> entry : message.permutationRanges.get(i).entrySet()) {
				Character key = entry.getKey();
				Character[][] value = entry.getValue();
				System.out.println(key+" : "+Arrays.deepToString(value));
			}
		}

		 */



		//System.out.println(message.permutationRanges);
		boolean found = false;
		//System.out.println("Character: "+Arrays.deepToString(message.permutationRanges.get(0).get('A')));
		//System.out.println("message.hint: "+message.hint);
		for (Map<Character, Character[][]> permutations : message.permutationRanges) {

			//System.out.println(message.permutationRanges);
			Character key = (Character) permutations.keySet().toArray()[0]; //the symbol of the permutation range
			//System.out.println("key: "+key);
			Character[] startPermutation = permutations.get(key)[0].clone(); //referenzen --> clone
			Character[] endPermutation = permutations.get(key)[1].clone(); //referenzen --> clone
			// Anmerkung:
			// StartPermutation zu einem Key ist immer das erratene Passwort ohne diesen Key
			Character[] currentPermutation = startPermutation;
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
						//message.permutationRanges = rangesCopy;
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
}