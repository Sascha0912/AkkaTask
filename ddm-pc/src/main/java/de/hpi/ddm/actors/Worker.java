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

	private static final Character[] passwordCharsAsArray = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'};
	public static final ArrayList<Character> PASSWORD_CHARS = new ArrayList(Arrays.asList(passwordCharsAsArray));
	public static final int PASSWORD_LENGTH = 10;

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
		private String hint;
		private char symbolNotInUniverse;
		private ActorRef sender;

		public HintMessage(String hint, char symbolNotInUniverse, ActorRef sender) {
			this.hint = hint;
			this.symbolNotInUniverse = symbolNotInUniverse;
			this.sender = sender;
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

	private static Set convertToSet(String string) {
		Set resultSet = new HashSet();
		for (int i = 0; i < string.length(); i++) {
			resultSet.add(new Character(string.charAt(i)));
		}

		return resultSet;
	}


	/*
	 * TODO:
	 * 1. Ranges für die Worker festelegen
	 * 2. 1. Durchlauf implementieren
	 * 3. n. Durchlauf implementieren
	 *
	*/

	/*
	private void handle(HintMessage message) {


	}
	*/

	private void handle(HintMessage message) {
		ActorRef sender = message.sender;
		ActorSelection senderProxy = this.context().actorSelection(sender.path().child(DEFAULT_NAME));

		String hint = message.hint;

		//TODO: implement
		boolean found = false;
		String candidate;

		//int x = 0;
		//Map<Integer,Integer> map = new TreeMap<>();
		//map.put(1,1);
		// long time = System.currentTimeMillis();;
		while (!found) {

			// used new Character because else PASSWORD_CHARS toArray is Object
			Character[] candidateAsArray = PASSWORD_CHARS.toArray(new Character[PASSWORD_CHARS.size()]);
			candidate = (candidateAsArray).toString(); //first candidate, i.e. abcdefghijk


			String hashedCandidate = this.hash(candidate);
			//if found, reduce candidate universe
			if (hashedCandidate.equals(hint)) {
				found = true;
				Set<Character> candidateAsSet = convertToSet(candidate);
				Set<Character> passwordCharsAsSet = new HashSet<>(PASSWORD_CHARS);

				passwordCharsAsSet.removeAll(candidateAsSet);

				char symbolNotInPassword = (char) passwordCharsAsSet.toArray()[0];

				HintMessage result = new HintMessage(hint, symbolNotInPassword, this.self());
				senderProxy.tell(result, this.self());
			} else {


				Util.findNextPermutation(candidateAsArray);

			}
		}

		System.out.println("Received hint " + hint);
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