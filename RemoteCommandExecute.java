import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
/*
 * Distributed Sorting and Computing Sum
 * 
 * @author : Suresh Babu Jothilingam
 * @author : Nitish Krishna Ganesan
 */

/*
 * RmoteCommandExecute thread is created to run the command seperately on given
 * slave machines
 */
public class RemoteCommandExecute extends Thread{
	String ip;
	/*
	 * constructor
	 */
	public RemoteCommandExecute(String ip) {
		// TODO Auto-generated constructor stub
		this.ip = ip;
	}
	
	/*
	 * run(), to copy the file to the slave machine
	 * compile and execute the dile
	 */
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		try{
			System.out.println(this.ip);
			JSch jsch = new JSch();
			Session session = jsch.getSession("pi", this.ip, 22);
			session.setPassword("raspberry");
			Properties prop = new Properties();
			prop.put("StrictHostKeyChecking", "no");
			session.setConfig(prop);
			session.connect();
			//To transfer file from local to remote
			Channel chTransfer = session.openChannel("exec");
			boolean timeStamp = true;
			//copy the file on slave machine
			String commandToTransfer = "scp " + (timeStamp ? "-p" :"") +" -t "+"SlaveNode.java";
			((ChannelExec)chTransfer).setCommand(commandToTransfer);
			InputStream input = chTransfer.getInputStream();
			OutputStream output = chTransfer.getOutputStream();
			chTransfer.connect();
			/*
			if(checkAck(input)!=0){
				System.exit(0);
			}
			*/
			File file = new File("SlaveNode.java");
			if(timeStamp){
				commandToTransfer="T"+(file.lastModified()/1000)+" 0";
				commandToTransfer+=(" "+(file.lastModified()/1000)+" 0\n"); 
				output.write(commandToTransfer.getBytes()); output.flush();
				/*
				if(checkAck(input)!=0){
					System.exit(0);
				}
				*/
			}

			long size = file.length();
			System.out.println(size);
			commandToTransfer = "C0644 "+size+" ";
			commandToTransfer += "SlaveNode.java";
			commandToTransfer+="\n";
			output.write(commandToTransfer.getBytes());
			output.flush();
			/*
			if(checkAck(input)!=0){
				System.exit(0);
			}
			*/
			FileInputStream inputStream = new FileInputStream(file);
			byte[] buffer = new byte[1024];
			while(true){
				int len=inputStream.read(buffer, 0, buffer.length);
				if(len<=0) break;
				output.write(buffer, 0, len);
			}
			inputStream.close();
			inputStream=null;
			buffer[0]=0; output.write(buffer, 0, 1); 
			output.flush();
			/*
			if(checkAck(input)!=0){
				System.exit(0);
			}
			*/
			output.close();
			chTransfer.disconnect();

			System.out.println("Finished copying file");
			//compile the file on slave machine
			Channel chCompile = session.openChannel("exec");
			((ChannelExec)chCompile).setCommand("javac SlaveNode.java");
			chCompile.connect();
			System.out.println("Compiled");
			chCompile.disconnect();
			//to execute remote command
			Channel chRun = session.openChannel("exec");
			((ChannelExec)chRun).setCommand("java SlaveNode");
			chRun.connect();
			System.out.println("Started Slave "+InetAddress.getByName(this.ip).getCanonicalHostName());
			input = chRun.getInputStream();
			chRun.connect();
			//get the input stream to view output on slave machine
			while(true){
				BufferedReader read = new BufferedReader(new InputStreamReader(input));
				while(input.available()>0){
					String line;
					if((line=read.readLine()) == null)
						break;
					else
						System.out.println(line);
				}
				this.sleep(1000);
			}

		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Error in executing command");
		}
	}

	public static void main(String ar[]){
		try{
			File file = new File(ar[0]);
			BufferedReader read = new BufferedReader(new FileReader(file));
			String line="";
			while((line = read.readLine())!=null)
				new RemoteCommandExecute(line).start();
		}catch(Exception e){
			e.printStackTrace();
		}
		 
	}
}
