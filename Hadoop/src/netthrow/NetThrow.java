package netthrow;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import mapreduce.QueryProcessor;
import socket.*;

public class NetThrow {

    private final int transferPort = 10021;
    private final int bufferLength = 1024;

    private boolean running;
    private File DownloadDirectory;
    private TCPServer tcpServer;
    private static Log log=LogFactory.getLog(NetThrow.class);	
    
    private Configuration conf;
    
    public NetThrow(Configuration c) throws Exception {
    	conf=c;
        String DownloadDirectoryPath = System.getProperty("user.home") + File.separator + "Downloads";
        DownloadDirectory = new File(DownloadDirectoryPath);
    }

    public void Start() throws Exception {
        new Thread(this::TransferServer).start();
        running = true;
        log.info("Server started.");
    }

    public void Stop() {
        try {
            tcpServer.Stop();
        } catch (Exception e) {
            e.printStackTrace();
        }

        running = false;
    }

    public boolean getRunning() {
        return running;
    }

    public void TransferServer() {
        try {
            tcpServer = new TCPServer(transferPort);

            while (tcpServer.running) {
                TCPConnection connection = tcpServer.Accept();
                new Thread(() -> HandlePut(connection)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tcpServer.Stop();
        }
    }

    private void HandlePut(TCPConnection connection) {
        try {
            Header header = receiveHeader(connection);

            switch (header.method) {
                case "PUT":
                    FileInfo fileInfo = header.file;
                    ReceiveFile(connection, new File(DownloadDirectory, Long.toHexString(Double.doubleToLongBits(Math.random()))), fileInfo.length);

                    break;
                default:
                    throw new ProtocolException();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Header receiveHeader(TCPConnection connection) throws IOException {
        String headerString = connection.ReceiveStringUntil("\n\n");
        return Header.DecodeHeader(headerString);
    }
    
    public String CheckVote(String cp,org.apache.hadoop.fs.FileSystem fs) throws IOException{
    	Path pa=new Path(cp+"/part-r-00000");
    	TreeMap<String,Integer> tm=new TreeMap<String,Integer>();
    	
    	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pa)));
    	
		String line;
		while((line=br.readLine()) != null){
			String[] str = line.split("\\s+");
			if(!tm.containsKey(str[1])){
				tm.put(str[1],1);
			}else{
				tm.put(str[1], tm.get(str[1])+1);
			}
		}	
		int maxA=0;
		String ans="";
		for(Map.Entry<String,Integer> entry :tm.entrySet()){
			if(entry.getValue()>maxA){
				ans=entry.getKey();
				maxA=entry.getValue();
			}
		}
		return ans;
    }

    public void ReceiveFile(TCPConnection connection, File file, long length) throws Exception {
        byte[] buffer = new byte[bufferLength];
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            long currentLength = 0;
            while (currentLength < length) {
                int readLength = connection.Receive(buffer, (int)Math.min(bufferLength, length - currentLength));
                fileOutputStream.write(buffer, 0, readLength);
                if (readLength == 0) throw new IOException();
                currentLength += readLength;
            }
        }
        log.info("File received in " + file.getAbsolutePath());
        org.apache.hadoop.fs.FileSystem fs=org.apache.hadoop.fs.FileSystem.get(conf);
        String input="hdfs://"+QueryProcessor.HDFSTmpPath+"/"+file.getName();
        String tmpoutdir="hdfs://"+QueryProcessor.HDFSTmpPath+"/"+file.getName()+"Out";
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(input));
        String[] args=new String[2];
        args[0]=input;
        args[1]=tmpoutdir;
        int success=ToolRunner.run(conf, new QueryProcessor(),args);
        log.info(success);
        if(success==0){
        	log.info(CheckVote(tmpoutdir, fs));
        }
    }
}