
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
/*
 * Distributed Sorting and Computing Sum
 * 
 * @author : Suresh Babu Jothilingam
 * @author : Nitish Krishna Ganesan
 */

/*
 * MasterNodeSum, main class to Perform the Distrubing Sum computing algorithm
 */

public class MasterNodeSum extends Thread{
	//Variable declaration
	static String inputDataFile;
	static HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
	static HashMap<Character, Integer> characterSumUnique = new HashMap<Character, Integer>();
	static HashMap<Character, Integer> characterCountUnique = new HashMap<Character, Integer>();
	static HashMap<Character, ArrayList<Integer>> sumMap = new HashMap<Character, ArrayList<Integer>>();
	static ArrayList<String> uniqueWords = new ArrayList<String>();
	static ArrayList<String> chunks = new ArrayList<String>();
	static ArrayList<String> slaves = new ArrayList<String>();
	static ArrayList<String> activeSlaves = new ArrayList<String>();
	static ArrayList<String> deactiveSlaves = new ArrayList<String>();
	static ArrayList<String> receivedChunks = new ArrayList<String>();
	static DecimalFormat decimalFormat = new DecimalFormat("#.##");
	static int maxBuffer = 10000;
	static int totalWords;
	static boolean isSumCount;
	static int count;
	Object sync = new Object();
	
	/*
	 * init() method to create folders to store chunks while the chunks are created
	 */
	public void init(){
		count = 0;
		isSumCount = false;
		File file = new File("chunks");
		if(!file.exists())
			file.mkdirs();
	}
	/*
	 * readSlaveIp, Method to read slave ip from text file and store it in arrayList
	 * @param fileName - name of the text file containing slave ip address
	 */
	public void readSlaveIp(String fileName){
		try{
			File file = new File(fileName);
			BufferedReader read = new BufferedReader(new FileReader(file));
			String line="";
			while((line = read.readLine()) != null){
				slaves.add(line);
				activeSlaves.add(line);
			}
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	/*
	 * getCount(), to get the number of lines in the input file
	 */
	public int getCount(){
		int count = 0;
		File file = new File(inputDataFile);
		try {
			BufferedReader read = new BufferedReader(new FileReader(file));
			while(read.readLine() != null)
				count++;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}

	/*
	 * createChunk(), Method to read input data createChunks, store chunk details and send it to the slaves
	 */
	public void createChunk(){
		int slaveCount = 0;
		//try block to handle file failure
		try{
			File file = new File(inputDataFile);
			BufferedReader read = new BufferedReader(new FileReader(file));
			totalWords = getCount(); 
			//compute the size of chunk for each slave
			int bigChunkSize = totalWords / slaves.size();
			int chunkCount = 0;
			int endIndex = 0;
			while(slaveCount < slaves.size()){
				//if this chunk is for last slave
				if(slaveCount == (slaves.size() - 1))
					bigChunkSize = totalWords - endIndex;

				//read till bigchunk size, partion it and send it to a slave
				int countBigChunk = 0, countSmallChunk = 0;
				//to sent the size of small chunk
				boolean flag = true;
				File chunkFile = null;
				PrintWriter writeFile = null;

				//check if ip already present in map
				map.put(slaves.get(slaveCount), new ArrayList<String>());	
				//to create a chunk no bigger than assigned size
				while(countBigChunk < bigChunkSize){
					if(countSmallChunk == maxBuffer){
						countSmallChunk = 0;
						flag = true;
						writeFile.close();
					}
					//assign the size for each small block
					//create chunkFiles
					if(flag){
						//logic
						//creates a file
						chunkFile = new File("chunks"+File.separator+chunkCount+".txt");
						writeFile = new PrintWriter(new BufferedWriter(new FileWriter(chunkFile)));
						//add this chunk index to map
						ArrayList<String> chunkId = map.get(slaves.get(slaveCount));
						chunkId.add(chunkCount+".txt");
						map.put(slaves.get(slaveCount), chunkId);
						flag = false;
						chunks.add(chunkCount+".txt");
						//keep track of number of chunks created
						chunkCount++;
					}
					String line = read.readLine();
					if(!line.isEmpty())
						writeFile.println(line);

					endIndex++;
					countSmallChunk++;
					countBigChunk++;

				}
				//close the printwriter 
				writeFile.close();
				//send the created chunks to respective slave
				new SendData(slaves.get(slaveCount), map.get(slaves.get(slaveCount))).start();
				slaveCount++;
			}
			read.close();
			//start the montioring after all the chunks have been sent 
			new Monitor().start();

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	/*
	 * printSum(), method to print the computed sum to the file
	 */
	public static synchronized void printSum(){
		//check to stop the monitor slaves
		if(!isSumCount){
			isSumCount = true;
			try{
				File file = new File("outputSumCount.txt");
				PrintWriter write = new PrintWriter(new FileWriter(file));
				for(Map.Entry<Character, ArrayList<Integer>> iterator : sumMap.entrySet()){
					write.println(iterator.getKey()+" "+iterator.getValue().get(0)+" "+iterator.getValue().get(1));
				}
				System.out.println("Computed");
				write.close();
				System.exit(0);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	/*
	 * sum(), method to add the received sum hash map from the slave
	 * @para receivedMap - hashmap received from slave for the sen tchunl
	 */
	public static synchronized void sum(HashMap<Character, ArrayList<Integer>> receivedMap){
		//reads the hashmap and adds it to the main map
		for(Map.Entry<Character, ArrayList<Integer>> iterator : receivedMap.entrySet()){
			if(sumMap.containsKey(iterator.getKey())){
				ArrayList<Integer> temp = new ArrayList<Integer>();
				ArrayList<Integer> countSum = sumMap.get(iterator.getKey());
				temp.add(countSum.get(0)+iterator.getValue().get(0));
				temp.add(countSum.get(1)+iterator.getValue().get(1));
				sumMap.put(iterator.getKey(), temp);
			} else{
				ArrayList<Integer> countSum = iterator.getValue();
				sumMap.put(iterator.getKey(), countSum);
			}
		}
		count++;		
		//if all the chunks have been received call the print function to output it to a file
		if(count == chunks.size()){
			System.out.println("Computing Sum and Count");
			printSum();
		}
	}
	/*
	 * retransmit(), to retransmit the files to new slave in case of failure
	 * @param ip - ip address of the dead slave
	 * @param files - files to be retransmitted
	 */
	public static synchronized void retransmit(String ip, ArrayList<String> files){
		//checks if the slave has been added to the dead slave list
		if(!deactiveSlaves.contains(ip)){
			activeSlaves.remove(ip); deactiveSlaves.add(ip);
			ArrayList<String> toBeSent = new ArrayList<String>();
			//compute the files to be sent
			for(int i=0;i<files.size();i++){
				if(!receivedChunks.contains(files.get(i))){
					toBeSent.add(files.get(i));
				}
			}
			//if there are any active slaves
			if(!activeSlaves.isEmpty()){
				//pick a random active slave
				String newIp = activeSlaves.get(new Random().nextInt(activeSlaves.size()));
				try{
					Socket socket = new Socket(newIp, 6666);
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					writer.println("sum");
					writer.println(toBeSent.size());
					for(int i=0;i<toBeSent.size();i++){
						File file = new File("chunks"+File.separator+toBeSent.get(i));
						BufferedReader read = new BufferedReader(new FileReader(file));
						writer.println(toBeSent.get(i));
						String line= "";
						while((line = read.readLine())!=null)
							writer.println(line);
						writer.println("end");
						read.close();
					}
					writer.close();
					socket.close();

				}catch(Exception e){
					//to handle failure during retransmit
					MasterNodeSort master = new MasterNodeSort();
					ArrayList<String> newIpFiles = map.get(newIp);
					newIpFiles.addAll(files);
					master.new ErrorHandler(newIp, newIpFiles).start();
				}
			}else{
				System.out.println("No active slaves");
			}
		}

	}

	/*
	 * Mointor class to mointor the slaves if they are active
	 */
	class Monitor extends Thread{
		String ip;
		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
			while(!isSumCount){
				try{
					for(int i=0;i<activeSlaves.size();i++){
						this.ip = activeSlaves.get(i);
						Socket socket = new Socket(this.ip, 6666);
						PrintWriter write  = new PrintWriter(socket.getOutputStream(), true);
						write.println("Alive?");
						write.close();
						socket.close();
						this.sleep(2000);
					}
				}catch(Exception e){
					//System.out.println("Error while monitoring "+this.ip);
					new ErrorHandler(this.ip, map.get(this.ip)).start();
				}

			}
		}

	}

	/*
	 * Senddata class is a thread which gets created during the chunk creation
	 * Files are sent as the chunks are created to acheive parallelism
	 */
	class SendData extends Thread{
		String ip;
		ArrayList<String> files;
		SendData(String ip, ArrayList<String> files){
			this.ip = ip;
			this.files = files;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
			try{
				Socket socket = new Socket(ip, 6666);
				PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
				writer.println("sum");
				writer.println(this.files.size());
				for(int i=0;i<this.files.size();i++){
					File file = new File("chunks"+File.separator+this.files.get(i));
					BufferedReader read = new BufferedReader(new FileReader(file));
					writer.println(this.files.get(i));
					String line= "";
					while((line = read.readLine())!=null){
						writer.println(line);
					}
					writer.println("end");
				}
				
				writer.close();
				socket.close();
			}catch(Exception e){
				//System.out.println("Error while sending data to "+this.ip);
				new ErrorHandler(this.ip, this.files).start();
			}
		}

	}
	/*
	 * Master Thread to handle incoming connections
	 * Creats thread for each connection to avoid blocking
	 */
	class MasterHandleConnection extends Thread{
		Socket socket;
		String ip;
		String chunkId;
		MasterHandleConnection(Socket socket){
			this.socket = socket;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
			this.ip = this.socket.getInetAddress().getHostAddress();
			try{
				ObjectInputStream read = new ObjectInputStream(this.socket.getInputStream());
				String request = read.readObject().toString();
				//check if the request message is sum
				if(request.equals("sum")){
					//this.chunkId = read.readLine();
					this.chunkId = read.readObject().toString();
					if(!receivedChunks.contains(this.chunkId)){
						HashMap<Character, ArrayList<Integer>> receivedMap = (HashMap<Character, ArrayList<Integer>>)read.readObject();
						synchronized (sync) {
							System.out.println("deleted");
							receivedChunks.add(this.chunkId);
							Files.delete(Paths.get(new File("chunks"+File.separator+this.chunkId).getAbsolutePath()));
						}
						sum(receivedMap);
					}

				}
				

			}catch(Exception e){
				new ErrorHandler(this.ip, map.get(this.ip));
			}
		}

	}
	/*
	 * ErrorHandler, general error handler thread class to handle all types of error
	 */
	class ErrorHandler extends Thread{
		String ip;
		ArrayList<String> files;
		public ErrorHandler(String ip, ArrayList<String> files) {
			// TODO Auto-generated constructor stub
			this.ip = ip;
			this.files = files;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
			retransmit(this.ip, this.files);
		}

	}

	/*
	 * run(), listeneing for incoming connections
	 * 
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		try{
			System.out.println("Master Server Started");
			ServerSocket server = new ServerSocket(6666);
			while(true){
				Socket socket = server.accept();
				new MasterHandleConnection(socket).start();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		MasterNodeSum master = new MasterNodeSum();
		master.init();
		master.readSlaveIp(args[0]);
		System.out.println(slaves);
		master.start();
		inputDataFile = args[1];
		master.createChunk();

		System.out.println("Chunk Creation Done");
	}

}
