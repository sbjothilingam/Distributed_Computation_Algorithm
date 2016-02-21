
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * MasterNodeSort, main class to Perform the Distrubing Sorting computing algorithm
 */
public class MasterNodeSort extends Thread{
	//Variable declarations
	static String inputDataFile;
	static HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
	static HashMap<Character, Integer> characterSumUnique = new HashMap<Character, Integer>();
	static HashMap<Character, Integer> characterCountUnique = new HashMap<Character, Integer>();
	static ArrayList<String> uniqueWords = new ArrayList<String>();
	static ArrayList<String> chunks = new ArrayList<String>();
	static ArrayList<String> slaves = new ArrayList<String>();
	static ArrayList<String> activeSlaves = new ArrayList<String>();
	static ArrayList<String> deactiveSlaves = new ArrayList<String>();
	static ArrayList<String> receivedChunks = new ArrayList<String>();
	static DecimalFormat decimalFormat = new DecimalFormat("#.##");
	static int maxBuffer = 10000;
	static int totalWords;
	static boolean isMergeCount;
	Object sync = new Object();
	
	/*
	 * init() method to create folders to store chunks while the chunks are created
	 */
	public void init(){
		isMergeCount = false;
		File file = new File("chunks");
		if(!file.exists())
			file.mkdirs();
		file = new File("chunks_received");
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
			//computes chunk size
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
				//to make sure chunks are no bigger than assigned size
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
			Files.delete(Paths.get(new File(inputDataFile).getAbsolutePath()));
			
			new Monitor().start();

		}catch(Exception e){
			e.printStackTrace();
		}
	}

	/*
	 * mergeAndCount(), to perform K-Way merge on the received sorted chunk files
	 * to get a final output file
	 */
	public static synchronized void mergeAndCount(){
		if(!isMergeCount){
			System.out.println("Merging");
			isMergeCount = true;
			ArrayList<BufferedReader> br = new ArrayList<BufferedReader>();
			try {
				PrintWriter brw = new PrintWriter(new FileWriter("outputSorted.txt"));
				// adding files to arraylist
				for(int i=0;i<chunks.size();i++){
					br.add(new BufferedReader(new FileReader("chunks_received"+File.separator+chunks.get(i))));
				}
				//merge
				while(br.size()>0){
					String smallest="";
					int smallestindex=0;
					ArrayList rem = new ArrayList();
					for(int i=0;i<br.size();i++){
						//mark the line so that it can be resetted
						br.get(i).mark(maxBuffer);
						String read;
						if((read = br.get(i).readLine())!=null){
							if(i==0){
								smallest = read;
							}else{
								String temp = read;
								if(!smallest.equals("")){
									if(temp.charAt(0)<smallest.charAt(0)){
										smallest = temp;
										smallestindex=i;
									}else if(temp.charAt(0)==smallest.charAt(0)){
										if(Integer.parseInt(temp.substring(1))<Integer.parseInt(smallest.substring(1))){
											smallest = temp;
											smallestindex=i;
										}
									}
								}
							}
						}else{
							rem.add(i);
						}

					}
					for(int i=0;i<br.size();i++){
						if(i!=smallestindex){
							//reset the reader if it is not smalledst
							br.get(i).reset();
						}
					}
					for(int i=0;i<rem.size();i++){
						br.remove((int)rem.get(i));
					}
					rem.clear();
					if(!smallest.equals("")){
						//System.out.println(smallest);
						brw.println(smallest);
						
					}
				}
				
				brw.close();

				System.out.println("Merged");
				
				//Free up the memory, delete the chunks in received folder
				for(int i=0;i<chunks.size();i++)
					Files.delete(Paths.get(new File("chunks_received"+File.separator+chunks.get(i)).getAbsolutePath()));

				System.exit(0);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/*
	 * retransmit(), to retransmit the files to new slave in case of failure
	 * @param ip - ip address of the dead slave
	 * @param files - files to be retransmitted
	 */
	public static synchronized void retransmit(String ip, ArrayList<String> files){
		//check if the slave is already added to the dead slave list
		if(!deactiveSlaves.contains(ip)){
			activeSlaves.remove(ip); deactiveSlaves.add(ip);
			ArrayList<String> toBeSent = new ArrayList<String>();
			for(int i=0;i<files.size();i++){
				if(!receivedChunks.contains(files.get(i))){
					toBeSent.add(files.get(i));
				}
			}
			//if there are any active slave
			if(!activeSlaves.isEmpty()){
				//pick a random slave and send
				String newIp = activeSlaves.get(new Random().nextInt(activeSlaves.size()));
				try{

					Socket socket = new Socket(newIp, 6666);
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					writer.println("chunk");
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
					//Handle failure while retransmitting
					MasterNodeSum master = new MasterNodeSum();
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
			while(!isMergeCount){
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
				writer.println("chunk");
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
				BufferedReader read = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
				String request = read.readLine();
				//if the request message is chunks
				if(request.equals("chunk")){
					this.chunkId = read.readLine();
					//add the received chunkid to a list
					if(!receivedChunks.contains(this.chunkId)){
						//System.out.println("Chunk received "+this.chunkId);
						File sortedChunkFile = new File("chunks_received"+File.separator+this.chunkId);
						PrintWriter write = new PrintWriter(new BufferedWriter(new FileWriter(sortedChunkFile)));
						String line = "";
						while(!(line = read.readLine()).equals("end"))
							write.println(line);
						write.close();
						read.close();
						synchronized (sync) {
							receivedChunks.add(this.chunkId);
					
						//delete the received chunk to free up memory
						Files.delete(Paths.get(new File("chunks"+File.separator+this.chunkId).getAbsolutePath()));
						}
					}
					//call the merge function after all the chunks are received
					if(receivedChunks.size() == chunks.size()){
						mergeAndCount();
					}
				}
				

			}catch(Exception e){
				//System.out.println("Error while receiving the chunk "+this.chunkId);
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
		MasterNodeSort master = new MasterNodeSort();
		master.init();
		master.readSlaveIp(args[0]);
		System.out.println(slaves);
		master.start();
		inputDataFile = args[1];
		master.createChunk();

		System.out.println("Chunk Creation Done");
	}

}
