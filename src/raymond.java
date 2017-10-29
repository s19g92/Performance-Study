/*
 * @author Shubham Gupta
 */

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings({ "rawtypes", "unused" })
class raymond {

	// Common Variables and Data Storage.
	static String coordinator = "";
	static String hostname = "";
	static int t1;
	static int t2;
	static int t3;
	static int no_process;
	static boolean is_coord = false;

	// Coordinator only data.
	static int active_process = 0;
	static int ready_process = 0;
	static int avg_msg_count = 0;
	static long avg_wait_time = 0;
	static long avg_delay = 0;
	static int complete;
	static Map<String, String> id_hostnames = new HashMap<String, String>();

	// Individual Process Variables.
	static String id = "";
	static int run_count = 0;
	static int[] LN;
	static int[] RN;
	static boolean has_token = false;
	static boolean in_cs = false;
	static boolean reg_complete = false;
	static boolean req_sent = false;
	static Socket socket;
	static List<String> queue = new ArrayList<String>();
	static Map<String, String> my_nebr_hostnames = new HashMap<String, String>();

	// Data Variables.
	static int msg_count = 0;
	static long wait_time;
	static long delay;
	static long request_time;

	/****************************** MAIN FUNCTION *********************************************/

	public static void main(String[] args) throws Exception {

		if (args.length != 0) {
			if (args[0].equalsIgnoreCase("-c")) {
				is_coord = true;
				id = "0";
				active_process++;
			}
			run_count = ThreadLocalRandom.current().nextInt(20, 30 + 1);
		}

		// Read the dsConfig File and assign the data to proper variables.
		Scanner sc2 = null;
		String[] list = null;
		try {
			sc2 = new Scanner(new File("SdsConfig"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while (sc2.hasNextLine()) {
			String next = sc2.nextLine();
			if (!next.isEmpty()) {
				list = next.split("\\s");
				if (list[0].equalsIgnoreCase("coordinator")) {
					coordinator = list[1];
					if (is_coord) {
						id_hostnames.put("0", coordinator + ".utdallas.edu");
						has_token = true;
					}
				} else if (list[0].equalsIgnoreCase("number")) {
					no_process = Integer.valueOf(list[3]);
					LN = new int[no_process];
					RN = new int[no_process];
				} else if (list[0].equalsIgnoreCase("t1")) {
					t1 = Integer.valueOf(list[1]);
				} else if (list[0].equalsIgnoreCase("t2")) {
					t2 = Integer.valueOf(list[1]);
				} else {
					if (list[0].equalsIgnoreCase("t3")) {
						t3 = Integer.valueOf(list[1]);
					}
				}
			}
		}
		System.out.println("Coordinator : " + coordinator);
		System.out.println("Is coordinator : " + is_coord);
		System.out.println("No. of process :" + no_process);
		System.out.println("Token:" + has_token);
		System.out.println("");

		// If the node is the coordinator then start listener.
		Thread start = new Thread() {
			public void run() {
				try {
					if (is_coord) {
						coord_register_new();
						recv_msg();
						// If the node is not the coordinator send register.
					} else {
						process_register();
						recv_msg();
					}
					System.out.println("Program thread exited");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		start.start();
	}

	// Send a message to another process.
	public static void send_msg(String host, int port, String sendMessage) {
		try {

			InetAddress address = InetAddress.getByName(host);
			socket = new Socket(address, port);

			// Send the message to the host
			OutputStream os = socket.getOutputStream();
			OutputStreamWriter osw = new OutputStreamWriter(os);
			BufferedWriter bw = new BufferedWriter(osw);

			bw.write(sendMessage + "!");
			// bw.flush();

		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// A message is received through a channel from another process.
	@SuppressWarnings("resource")
	public static void recv_msg() throws IOException {

		String message = "";
		try {

			int port = 25555;
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Process listening to the port 25555");

			// Process is listening for new messages after registeration.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();

				InputStream is = socket.getInputStream();
				String hostName = socket.getInetAddress().getHostName();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				message = messageBuffer.toString();

				if (message != null && message != "") {
					System.out.println("Message received from " + hostName
							+ " is " + message);
					final String msg = message;
					Thread one = new Thread() {
						public void run() {
							try {
								msg_type(msg);
							} catch (IOException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					};
					one.start();
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		} finally {
			// Closing the socket
			try {
				// socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Perform some action depending on the type of the message received.
	public static void msg_type(String message) throws IOException,
			InterruptedException {
		// On Receiving a hostnames from store in map.
		if (message.split(" ")[0].equalsIgnoreCase("hostnames")) {
			String[] list = message.split(" ");
			for (int i = 1; i < list.length - 1; i++) {
				String id = list[i];
				i++;
				if (!list[i].isEmpty()) {
					my_nebr_hostnames.put(id, list[i]);
				}
			}
			reg_complete = true;
			System.out.print(my_nebr_hostnames);
			System.out.println("");
		}

		// On Receiving compute message.
		else if (message.split(" ")[1].equalsIgnoreCase("compute")) {
			compute();
		}

		// On receiving request message.
		else if (message.split(" ")[0].equalsIgnoreCase("request")) {
			String[] list = message.split(" ");
			int number = Integer.valueOf(list[2]);
			int q = Integer.valueOf(list[1]);
			RN[q] = Math.max(RN[q], number);

			if (has_token && !in_cs) {
				if (RN[q] == LN[q] + 1 && queue.isEmpty()) {
					long time = System.currentTimeMillis() % 1000;
					String msg = "Token " + time + " " + LN + " " + queue;
					send_msg(id_hostnames.get(list[1]), 25555, msg);
					has_token = false;
					System.out.println("SENT : " + msg);
				}
			}
		}

		// On receiving token
		else if (message.split(" ")[0].equalsIgnoreCase("request")) {

			has_token = true;
			String[] list = message.split(" ");
			wait_time = request_time - Long.parseLong(list[1]);
			delay = System.currentTimeMillis() % 1000 - Long.parseLong(list[1]);

			for (int i = 0; i < no_process; i++) {
				LN[i] = Integer.valueOf(i + 2);
			}

			if (list.length > no_process + 2)
				for (int i = no_process + 2; i < list.length; i++) {
					queue.add(list[i]);
				}

			if (req_sent) {
				req_sent = false;
				start_cs();
			}
		}

		// On receiving data from other process.
		else if (message.split(" ")[0].equalsIgnoreCase("data")) {
			String[] list = message.split(" ");
			if (avg_msg_count == 0) {
				avg_msg_count = Integer.valueOf(list[1]);
			} else {
				avg_msg_count = (avg_msg_count + Integer.valueOf(list[1])) / 2;
			}

			if (avg_delay == 0) {
				avg_delay = Long.parseLong(list[2]);
			} else {
				avg_delay = (avg_delay + Long.parseLong(list[2])) / 2;
			}

			if (avg_wait_time == 0) {
				avg_wait_time = Integer.valueOf(list[1]);
			} else {
				avg_wait_time = (avg_wait_time + Long.parseLong(list[2])) / 2;
			}
		}

		// On receiving completed from process.
		else if (message.equalsIgnoreCase("completed")) {
			complete++;
			if (complete == no_process) {
				Iterator it = id_hostnames.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String id_proc = (String) pair.getKey();
					String host = (String) pair.getValue();
					String msg = "TERMINATE";
					if (!id_proc.equalsIgnoreCase("0")) {
						send_msg(host, 25555, msg);
						System.out.println("SENT " + msg + " to " + host);
					}

				}
				System.out.println("AVG MSG COUNT" + avg_msg_count);
				System.out.println("AVG DELAY" + avg_delay);
				System.out.println("AVG WAIT TIME" + avg_wait_time);
				System.exit(0);
			}
			
		}

		// On receiving terminate.
		else if (message.equalsIgnoreCase("terminate")) {
			System.exit(0);
		}
	}

	// Send the inital register message to the coordinator
	// if the process itself is not the coordinator.
	public static void process_register() throws IOException {

		if (!is_coord) {
			String msg = "REGISTER";
			int port = 25555;
			String host = coordinator + ".utdallas.edu";
			try {

				InetAddress address = InetAddress.getByName(host);
				socket = new Socket(address, port);

				// Send the message to the host
				OutputStream os = socket.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os);
				BufferedWriter bw = new BufferedWriter(osw);

				bw.write(msg + "!");
				bw.flush();
				System.out.println("Message " + msg + " sent to the : " + host);

				// Get the return message from the coordinator.
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);

				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				// message is complete!
				String message = messageBuffer.toString();

				if (message != null) {
					System.out.println("Message received : " + message);
					String[] msg_list = message.split(" ");
					if (msg_list[0].equalsIgnoreCase("ack")) {

						// Check the id assigned and store the corresponding
						// values.
						id = msg_list[1];
						System.out.println("Id received : " + id);
					}
				}

			} catch (Exception exception) {
				exception.printStackTrace();
			} finally {
				// Closing the socket
				try {
					// socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Processing the register requests received from
	// non-coordinator processes.
	public static void coord_register_new() throws IOException {

		try {

			int port = 25555;
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(port);
			System.out.println("Coordinator listening to the port 25555");

			// Coordinator is listening always for new process registering.
			while (true) {

				// Reading the message from the other process.
				socket = serverSocket.accept();

				String hostName = socket.getInetAddress().getHostName();
				InputStream is = socket.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				final char endMarker = '!';

				// StringBuffer if you need to be threadsafe
				StringBuilder messageBuffer = new StringBuilder();
				int value;
				// reads to the end of the stream or till end of message
				while ((value = br.read()) != -1) {
					char c = (char) value;
					if (c == endMarker) {
						break;
					} else {
						messageBuffer.append(c);
					}
				}
				String message = messageBuffer.toString();

				if (message != null) {

					// Create a new thread for every message received to process
					// it after register is complete. Else, go to register.
					if (reg_complete) {
						System.out.println("Message from " + hostName + " is "
								+ message);
						final String msg = message;
						Thread yz = new Thread() {
							public void run() {
								try {
									msg_type(msg);
								} catch (IOException e) {
									e.printStackTrace();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						};
						yz.start();
					}

					// Sending the process its id.
					else if (message.equalsIgnoreCase("register")) {
						System.out.println("Message from " + hostName + " is "
								+ message);
						String returnMessage;
						if (active_process < no_process) {

							// Assign a id to the process and send it the
							// information.
							// Also store its hostname.
							active_process++;
							String id = Integer.toString(active_process - 1);
							returnMessage = "ACK " + id;

							// Store the id and the hostname in the map.
							id_hostnames.put(id, hostName);

							// Sending the response back to the process.
							OutputStream os = socket.getOutputStream();
							OutputStreamWriter osw = new OutputStreamWriter(os);
							BufferedWriter bw = new BufferedWriter(osw);
							bw.write(returnMessage + "!");
							System.out.println("Message sent is : "
									+ returnMessage);
							// bw.flush();
						}
					}
				}
				if (active_process == no_process && !reg_complete) {
					System.out.println("All process registered !");
					send_hostnames();
					reg_complete = true;
					start_compute();
					compute();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				// socket.close();
			} catch (Exception e) {
			}
		}
	}

	// Send the hostnames of the neighbours to registered processes
	// once all processes have registered and their hostnames are stored.
	public static void send_hostnames() throws IOException {

		// If all processes sent the register message and coordinator
		// knows everyone's hostname.
		if (active_process == no_process) {

			// Coordinator registers its own neighbours
			if (is_coord) {
				my_nebr_hostnames = id_hostnames;
			}

			// Send hostnames of neightbours to processes.
			for (int i = 1; i <= no_process - 1; i++) {
				String message = "Hostnames";

				Iterator it = id_hostnames.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String id = (String) pair.getKey();
					String host = (String) pair.getValue();
					message = message + " " + id + " " + host;

				}
				send_msg(id_hostnames.get("" + i), 25555, message);
			}
		}
	}

	// Send the compute message after receiving all ready messages.
	public static void start_compute() {

		if (ready_process == no_process) {
			Iterator it = id_hostnames.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				String id_proc = (String) pair.getKey();
				String host = (String) pair.getValue();
				String msg = id + " COMPUTE";
				if (!id_proc.equalsIgnoreCase("0")) {
					send_msg(host, 25555, msg);
					System.out.println("SENT " + msg + " to " + host);
				}
			}
		}
	}

	// Simulates the computing process of a node on receiving a message.
	public static void compute() {
		try {
			// Randomly sleep the thread for upto 5ms.
			int randomNum = ThreadLocalRandom.current().nextInt(t1, t2 + 1);
			Thread.sleep(randomNum);

			// After waking up request critical section if it doesnt have token
			if (!has_token) {
				String msg = "";
				Iterator it = my_nebr_hostnames.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pair = (Map.Entry) it.next();
					String id_proc = (String) pair.getKey();
					String host = (String) pair.getValue();
					request_time = System.currentTimeMillis() % 1000;
					RN[Integer.valueOf(id)]++;
					msg_count++;
					msg = "Request " + id + " " + RN[Integer.valueOf(id)];
					req_sent = true;
					if (!id_proc.equalsIgnoreCase("0")) {
						send_msg(host, 25555, msg);
					}
				}
				System.out.println(msg);
			} else {
				RN[Integer.valueOf(id)]++;
				start_cs();
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void start_cs() throws InterruptedException {

		in_cs = true;
		if (!is_coord) {
			String data_msg = "DATA " + msg_count + " " + delay + " "
					+ wait_time;
			send_msg(id_hostnames.get("0"), 25555, data_msg);
		}
		else {
			if (avg_msg_count == 0) {
				avg_msg_count = msg_count;
			} else {
				avg_msg_count = (avg_msg_count + msg_count) / 2;
			}

			if (avg_delay == 0) {
				avg_delay = delay;
			} else {
				avg_delay = (avg_delay + delay) / 2;
			}

			if (avg_wait_time == 0) {
				avg_wait_time = wait_time;
			} else {
				avg_wait_time = (avg_wait_time + wait_time) / 2;
			}
		}
		// Reset data
		msg_count = 0;
		delay = 0;
		wait_time = 0;

		System.out.println("ENTERING CS !!");
		Thread.sleep(t3);
		System.out.println("EXITING CS !!");

		// After waking up, increase own counter and add to queue.
		int p = Integer.valueOf(id);
		LN[p] = RN[p];
		for (int i = 0; i < no_process; i++) {
			boolean in_queue = false;

			Iterator it = my_nebr_hostnames.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				String id_proc = (String) pair.getKey();
				if (id_proc.equalsIgnoreCase("" + i)) {
					in_queue = true;
					break;
				}
			}
			if (!in_queue && RN[i] == LN[i] + 1) {
				queue.add("" + i);
			}
		}

		// If queue is not empty. Send the token.
		if (!queue.isEmpty()) {
			String i = queue.get(0);
			queue.remove(0);
			long time = System.currentTimeMillis() % 1000;
			String msg = "Token " + time + " " + LN + " " + queue;
			send_msg(id_hostnames.get(i), 25555, msg);
			has_token = false;
			System.out.println("SENT : " + msg);
		}
		in_cs = false;

		run_count--;
		if (run_count > 0) {
			compute();
		} else {
			if (!id.equalsIgnoreCase("0"))
				send_msg(id_hostnames.get("0"), 25555, "completed");
			else {
				complete++;
				if (complete == no_process) {
					Iterator it = id_hostnames.entrySet().iterator();
					while (it.hasNext()) {
						Map.Entry pair = (Map.Entry) it.next();
						String id_proc = (String) pair.getKey();
						String host = (String) pair.getValue();
						String msg = "TERMINATE";
						if (!id_proc.equalsIgnoreCase("0")) {
							send_msg(host, 25555, msg);
							System.out.println("SENT " + msg + " to " + host);
						}
					}
					System.out.println("AVG MSG COUNT" + avg_msg_count);
					System.out.println("AVG DELAY" + avg_delay);
					System.out.println("AVG WAIT TIME" + avg_wait_time);
					System.exit(0);
				}
				
			}
		}
	}
}