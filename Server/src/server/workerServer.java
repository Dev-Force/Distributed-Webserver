package server;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class workerServer extends Thread {
        
    
    Socket clientSocket = null;
    String status = null;
    PrintWriter out = null;
    BufferedReader in = null;

    
    public workerServer(Socket clientSocket) {
        try {
            this.clientSocket = clientSocket;
            this.clientSocket.setSoTimeout(4000);
            try {
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            } catch (IOException ex) {
                Logger.getLogger(workerServer.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SocketException ex) {
            Logger.getLogger(workerServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public String sendGet(String key) throws IOException {
        out.println(key);
        try {
            return in.readLine();
        }
        catch(SocketException ex) {
            System.out.println("Delayyyy");
            return "**";//network delays
        }
    }

    
    public String sendPut(String key, String value) throws IOException {
        out.println(key + "*" + value);
        try {
            return in.readLine();
        }
        catch(SocketException ex) {
            System.out.println("Delay!");
            return "**";//network delays or disconnected node
        }
    }
    
    
    public boolean noError() {
        return !out.checkError();
    }
    
    
} // End workerServer
