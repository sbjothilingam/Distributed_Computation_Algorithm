import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
/*
 * Distributed Sorting and Computing Sum
 * 
 * @author : Suresh Babu Jothilingam
 * @author : Nitish Krishna Ganesan
 */

public class SlaveNode extends Thread{
	
	/*sendToMaster(), send the sorted data back to master
	 * 
	 */
	public static synchronized void sendToMaster(String ip, String chunkId,ArrayList<String> data){
		try{
			Socket socket = new Socket(ip, 6666);
			PrintWriter write = new PrintWriter(socket.getOutputStream(), true);
			write.println("chunk");
			write.println(chunkId);
			for(int i=0;i<data.size();i++){
				write.println(data.get(i));
			}
			write.println("end");
			write.close();
			socket.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	/*sendToMaster(), send the computed sum back to master
	 * 
	 */
	public static synchronized void sendToMaster(String ip, String chunkId, HashMap<Character, ArrayList<Integer>> data){
		try{
			Socket socket = new Socket(ip, 6666);
			ObjectOutputStream write = new ObjectOutputStream(socket.getOutputStream());
			write.writeObject("sum");
			write.writeObject(chunkId);
			write.writeObject(data);
			write.close();
			socket.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	/*
	 * computeSum(), to compute the sum of the prefix character 
	 * @para data - input chunk data
	 * @return sumCount - hashmap containing the sum
	 */
	public static synchronized HashMap<Character, ArrayList<Integer>> computeSum(ArrayList<String> data){
		HashMap<Character, ArrayList<Integer>> sumCount = new HashMap<Character, ArrayList<Integer>>();
		for(String word : data){
			if(sumCount.containsKey(word.toUpperCase().charAt(0))){
				ArrayList<Integer> temp = new ArrayList<Integer>();
				ArrayList<Integer> value = sumCount.get(word.toUpperCase().charAt(0));
				//value.add(0, value.get(0)+1);
				//value.add(1,value.get(1)+Integer.parseInt(word.substring(1)));
				temp.add(value.get(0)+1);
				temp.add(value.get(1)+Integer.parseInt(word.substring(1)));
				sumCount.put(word.toUpperCase().charAt(0), temp);
			} else{
				ArrayList<Integer> value = new ArrayList<Integer>();
				value.add(1); value.add(Integer.parseInt(word.substring(1)));
				sumCount.put(word.toUpperCase().charAt(0), value);
			}
		}
		return sumCount;
	}
	/*quickSort(), to perform quick sort on the list received
	 * @para data - input data
	 */
	public static synchronized ArrayList<String> quickSort(ArrayList<String> data){
		ArrayList<String> sorted = new ArrayList<String>();
		ArrayList<String> medianOfThree = new ArrayList<String>();
		ArrayList<String> left = new ArrayList<String>();
		ArrayList<String> right = new ArrayList<String>();
		ArrayList<String> middle = new ArrayList<String>();

		if(data.size() < 2)
			return data;
		//median of three to get best split
		medianOfThree.add(data.get(0)); medianOfThree.add(data.get(data.size() - 1));
		medianOfThree.add(data.get(data.size()/2));
		Collections.sort(medianOfThree);
		String median = medianOfThree.get(1);

		for(int i=0;i<data.size();i++){
			int compare = 0;
			if(data.get(i).charAt(0) > median.charAt(0)){
				compare = 1;
			} else if(data.get(i).charAt(0) < median.charAt(0)){
				compare = -1;
			} else if(data.get(i).charAt(0) == median.charAt(0)){
				if(Integer.parseInt(data.get(i).substring(1)) > Integer.parseInt(median.substring(1))){
					compare = 1;
				} else if(Integer.parseInt(data.get(i).substring(1)) < Integer.parseInt(median.substring(1))){
					compare = -1;
				} else{
					compare = 0;
				}
			}
			
			if(compare < 0){
				left.add(data.get(i));
			} else if(compare > 0){
				right.add(data.get(i));
			} else{
				middle.add(data.get(i));
			}
		}

		left = quickSort(left);
		for(int i=0;i<middle.size();i++)
			left.add(middle.get(i));
		right = quickSort(right);

		for(int i=0;i<left.size();i++)
			sorted.add(left.get(i));
		for(int i=0;i<right.size();i++)
			sorted.add(right.get(i));

		return sorted;
	}


	/*
	 * run(), listeneing for incoming connections
	 * 
	 */
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		try{
			ServerSocket serverSocket = new ServerSocket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(6666));
			System.out.println("Slave Server Started");
			while(true){
				Socket socket = serverSocket.accept();
				new SlaveServer(socket).start();
			}

		} catch(Exception e){
			e.printStackTrace();
		}
	}
	/*
	 * SlaverServer thread class which is created to handle each and every connection
	 */
	class SlaveServer extends Thread{
		Socket socket;
		String chunkId;
		public SlaveServer(Socket socket) {
			// TODO Auto-generated constructor stub
			this.socket = socket;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
			try{
				BufferedReader read = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
				String message = read.readLine();
				//if message is chunk do the sorting
				if(message.equals("chunk")){
					int size = Integer.parseInt(read.readLine());
					System.out.println(size);
					
					for(int i=0;i<size;i++){
						ArrayList<String> receivedData = new ArrayList<String>();
						this.chunkId = read.readLine();
						System.out.println("Receving "+this.chunkId);
						String line = "";
						while(!(line = read.readLine()).equals("end"))
							receivedData.add(line);
						System.out.println("Chunk received "+this.chunkId);
						sendToMaster(this.socket.getInetAddress().getHostAddress(), this.chunkId, quickSort(receivedData));
						//System.out.println("Sent "+this.chunkId);
					}
					//if the message is sum do the sum
				}else if(message.equals("sum")){
					int size = Integer.parseInt(read.readLine());
					System.out.println(size);
					
					for(int i=0;i<size;i++){
						ArrayList<String> receivedData = new ArrayList<String>();
						this.chunkId = read.readLine();
						System.out.println("Receving "+this.chunkId);
						String line = "";
						while(!(line = read.readLine()).equals("end"))
							receivedData.add(line);
						System.out.println("Chunk received "+this.chunkId);
						sendToMaster(this.socket.getInetAddress().getHostAddress(), this.chunkId, computeSum(receivedData));
						//System.out.println("Sent "+this.chunkId);
					}
					
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}
	public static void main(String[] args){
		new SlaveNode().start();
	}
}
