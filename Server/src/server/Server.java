package server;


import com.sun.net.httpserver.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.Random;


public class Server {
    
    //static ArrayList<Integer> disconnectedNodeID = new ArrayList<Integer>();
    static ArrayList<workerServer> workerList = new ArrayList<workerServer>();
    
    static HashMap keyWorker = new HashMap(); // key from user, value the id of the worker
    static HashMap keyValue = new HashMap();  // key, value for worker at server

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    FileOutputStream fos = new FileOutputStream("serverkeyworker.backup");
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    oos.writeObject(keyWorker);
                    oos.close();
                    fos.close();
                  
                    fos = new FileOutputStream("serverkeyvalue.backup");
                    oos = new ObjectOutputStream(fos);
                    oos.writeObject(keyValue);
                    oos.close();
                    fos.close();

                    fos = new FileOutputStream("serverarraysize.backup");
                    oos = new ObjectOutputStream(fos);
                    oos.writeObject(workerList.size());
                    oos.close();
                    fos.close();

                    
                }
                catch(IOException ioe) {
                      ioe.printStackTrace();
                }
            }
        });
        
        try
        {
            File file = new File("serverkeyworker.backup");
            File file2 = new File("serverkeyvalue.backup");
            File file3 = new File("serverarraysize.backup");
            if(file.exists() && file2.exists() && file3.exists()) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    keyWorker = (HashMap) ois.readObject();
                    ois.close();
                }
                try (FileInputStream fis = new FileInputStream(file2)) {
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    keyValue = (HashMap) ois.readObject();
                    ois.close();
                }
                try (FileInputStream fis = new FileInputStream(file3)) {
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    int size = (int) ois.readObject();
                    for(int i=0; i<size; i++) {
                        workerList.add(null);
                    }
                    ois.close();
                }
            }
            else {
                System.out.println("some file exist and some dont. delete all files related to the server and then try again");
            }
        }catch(IOException ioe) {
           ioe.printStackTrace();
           return;
        }catch(ClassNotFoundException c) {
           System.out.println("Class not found");
           c.printStackTrace();
           return;
        }
        
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(4444), 0);
            server.createContext("/store", new RequestHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
            
            PrintWriter out = null;
            BufferedReader in = null;
            
            ServerSocket serverSocket = new ServerSocket(5555);
            Socket clientSocket;

            while (true) {
                clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(4000);
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String workerResponse;
                
                workerServer wt = new workerServer(clientSocket);
                
                try {
                    workerResponse = in.readLine();
                }
                catch(NullPointerException e) {
                    workerResponse = "**";
                }
                
                if(workerResponse.equals("**")) {
                    workerList.add(wt);
                    out.println(workerList.size()-1);
                }
                else {
                    workerList.set(Integer.parseInt(workerResponse), wt);
                }
                
                wt.start();
            }            
        } catch (IOException ex) {
            Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    } // End main

    
    private static class RequestHandler implements HttpHandler {
        
        @Override
        public void handle(HttpExchange t) throws IOException {
            
            // String response = "<?xml version='1.0' encoding='utf-8' ?>";
            
            String response = "";
            
            // String response = "Hello World!\n\nServer Respond!\n\n"; // Responde message
            
            // Tell users which are the acceptable patterns (and therefore action)
            // response = response + "GET: ip:port/store/key=<value>\n";
            // response = response + "PUT: ip:port/store/key=<value>&value=<value>\n\n";
            
            String uri = (t.getRequestURI()).toString(); // Get uri from url
            
            // Acceptable Patterns
            String p1 = "^/store/$";                                        // '/store/'
            String p2 = "^/store/key=[_A-Za-z0-9-]+$";                      // '/store/key=<value>'
            String p3 = "^/store/key=[_A-Za-z0-9-]+&value=[_A-Za-z0-9-]+$"; // '/store/key=<value>&value=<value>'
            
            String[] urlPieces;     // URI Pieces     (/store/key=<value> or /store/key=<value>&value=<value>) 
            String[] requestPieces; // Request Pieces (key=<value> or key=<value>&value=<value>) 
            String[] keyPieces;     // Key Pieces     (key=<value>)
            String[] valuePieces;   // Value Pieces   (value=<value>)
            String key;             // Key
            String value;           // Value
            
            if(Pattern.matches(p1, uri)) { // if pattern is /store/
                // response = response + "NO ACTION!\n\n";
                response = "<?xml version='1.0' encoding='utf-8'?>"
                        + "<response status='fail' >"
                        + "<error code='100' msg='No Action' prompt='Request Data Using Right Expressions' />"
                        + "</response>";
            }
            else if(Pattern.matches(p2, uri)) { // if pattern is /store/key=<value>
                
                urlPieces = uri.split("/");
                
                requestPieces = urlPieces[2].split("&");
                
                keyPieces = requestPieces[0].split("=");    
                
                key = keyPieces[1];
                
                int workerNum;
                String result;
                    
                try {
                    
                    workerNum = Integer.parseInt(keyWorker.get(key).toString());
                    
                    if( workerNum == -1) { // Worker at server
                        result = keyValue.get(key).toString();
                    }
                    else {
                        
                        result = workerList.get(workerNum).sendGet(key);
                        
                        if(result.equals("**")) { // In case worker is delaying or is disconnected
                            result = keyValue.get(key).toString();
                        }
                        
                    }
                    
                    response = "<?xml version='1.0' encoding='utf-8'?>"
                            + "<response status='ok' fa='get' >"
                            + "<entity>"
                            + "<key>" + key + "</key>"
                            + "<value>" + result + "</value>"
                            + "</entity>"
                            + "</response>";
                    
                }
                catch(NullPointerException e) {
                    
                    try {
                        result = keyValue.get(key).toString();
                        response = "<?xml version='1.0' encoding='utf-8'?>"
                                + "<response status='ok' fa='get' >"
                                + "<entity>"
                                + "<key>" + key + "</key>"
                                + "<value>" + result + "</value>"
                                + "</entity>"
                                + "</response>";
                    }
                    catch(NullPointerException e2) {
                        response = "<?xml version='1.0' encoding='utf-8'?>"
                                + "<response status='fail' >"
                                + "<error code='204' msg='Not Found!' prompt='Search for the Right Key' />"
                                + "</response>";
                    }
                    
                }
                
            }
            else if(Pattern.matches(p3, uri)) { // if pattern is /store/key=<value>&value=<value>
                String result = null;
                urlPieces = uri.split("/");
                
                requestPieces = urlPieces[2].split("&");
                
                keyPieces = requestPieces[0].split("=");
                valuePieces = requestPieces[1].split("=");
                
                key = keyPieces[1];
                value = valuePieces[1];
                
                if(workerList.isEmpty()) {
                    keyWorker.put(key, -1);
                    keyValue.put(key, value);
                    
                    response = "<?xml version='1.0' encoding='utf-8'?>"
                            + "<response status='ok' fa='put' >"
                            + "<entity>"
                            + "<key>" + key + "</key>"
                            + "<value>" + value + "</value>"
                            + "<action>insert</action>"
                            + "</entity>"
                            + "</response>";
                }
                else {
                    
                    // Check if value exists in worker
                    String checkKeyValueWorker = "";
                    
                    try {
                        checkKeyValueWorker = keyWorker.get(key).toString();
                    }
                    catch(NullPointerException e1) {
                        checkKeyValueWorker = "*";
                    }
                    
                    if(checkKeyValueWorker.equals("*")) {
                    
                        Random randomGenerator = new Random();  
                        int randomInt;
                        randomInt = randomGenerator.nextInt(workerList.size()+1);

                        if(randomInt == workerList.size()) {

                            String workerSresult = ""; // Worker at Server result

                            try {
                                workerSresult = keyValue.get(key).toString();
                            }
                            catch(NullPointerException e2) {
                                workerSresult = "*";
                            }

                            keyWorker.put(key, -1);
                            keyValue.put(key, value);

                            if(workerSresult.equals("*")) {
                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                    + "<response status='ok' fa='put' >"
                                    + "<entity>"
                                    + "<key>" + key + "</key>"
                                    + "<value>" + value + "</value>"
                                    + "<action>insert</action>"
                                    + "</entity>"
                                    + "</response>";
                            }
                            else {
                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                    + "<response status='ok' fa='put' >"
                                    + "<entity>"
                                    + "<key>" + key + "</key>"
                                    + "<value>" + value + "</value>"
                                    + "<action>update</action>"
                                    + "</entity>"
                                    + "</response>";
                            }
                        }
                        else {

                            result = workerList.get(randomInt).sendPut(key, value);                        

                            if(result.equals("**")) { // In case worker is delaying or is disconnected
                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                        + "<response status='fail' >"
                                        + "<error code='202' msg='Delay or Disconnection of Node. Data can not be replaced!' prompt='Try Later!' />"
                                        + "</response>";
                            }
                            else {
                                keyWorker.put(key, randomInt); // Index key to worker num
                                keyValue.put(key, value);      // Save data to server/worker as back up. 
                                                               // We call get to hashmap only when workers don't respond but the key exists in keyWorker

                                String tempResult[]    = result.split("\\*");
                                String tempResult2     = tempResult[0]; // value
                                String tempResult2stat = tempResult[1]; // insert or update

                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                        + "<response status='ok' fa='put' >"
                                        + "<entity>"
                                        + "<key>" + key + "</key>"
                                        + "<value>" + tempResult2 + "</value>"
                                        + "<action>" + tempResult2stat + "</action>"
                                        + "</entity>"
                                        + "</response>";
                                
                            } // End else

                        } // End else
                        
                    } // End if checkKeyValueWorker equals *
                    else {
                        
                        int tempKey = Integer.parseInt(checkKeyValueWorker);
                        
                        if(tempKey == -1) {
                            
                            keyWorker.put(key, -1); // Index key to worker num
                            keyValue.put(key, value); // Save data to server/worker as back up. 
                                                      // We call get to hashmap only when workers don't respond but the key exists in keyWorker
                            
                            response = "<?xml version='1.0' encoding='utf-8'?>"
                                        + "<response status='ok' fa='put' >"
                                        + "<entity>"
                                        + "<key>" + key + "</key>"
                                        + "<value>" + value + "</value>"
                                        + "<action>update</action>"
                                        + "</entity>"
                                        + "</response>";
                            
                        }
                        else {
                            
                            result = workerList.get(tempKey).sendPut(key, value);                        

                            if(result.equals("**")) { // In case worker is delaying or is disconnected
                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                        + "<response status='fail' >"
                                        + "<error code='202' msg='Delay or Disconnection of Node. Data can not be replaced!' prompt='Try Later!' />"
                                        + "</response>";
                                
                            }
                            else {
                                keyWorker.put(key, tempKey); // Index key to worker num
                                keyValue.put(key, value);    // Save data to server/worker as back up. 
                                                             // We call get to hashmap only when workers don't respond but the key exists in keyWorker

                                String tempResult[]    = result.split("\\*");
                                String tempResult2     = tempResult[0]; // value
                                String tempResult2stat = tempResult[1]; // insert or update

                                response = "<?xml version='1.0' encoding='utf-8'?>"
                                        + "<response status='ok' fa='put' >"
                                        + "<entity>"
                                        + "<key>" + key + "</key>"
                                        + "<value>" + tempResult2 + "</value>"
                                        + "<action>" + tempResult2stat + "</action>"
                                        + "</entity>"
                                        + "</response>";
                                
                            } // End else
                            
                        } // End else (if tempKey == -1)
                        
                    } // End else (if checkKeyValueWorker equals *)
                    
                } // End else
                
            }
            else {
                
                response = "<?xml version='1.0' encoding='utf-8'?>"
                        + "<response status='fail' >"
                        + "<error code='100' msg='Expression Error!' />"
                        + "</response>";
                
            } // End else
            
            t.sendResponseHeaders(1000, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes()); // Send server respond!
            os.close();
            
        } // End handle
        
    } // End RequestHandler
    
} // End class Server