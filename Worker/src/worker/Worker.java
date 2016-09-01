package worker;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Worker {
    
    
    static int workerID;
    static HashMap keyValue = new HashMap();
    static Socket socket;
   
    
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    //System.out.println("Inside Add Shutdown Hook");
                    socket.close();
                } catch (IOException ex) {
                    Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                try {
                    FileOutputStream fos = new FileOutputStream("hashmap.backup");
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    oos.writeObject(keyValue);
                    oos.close();
                    fos.close();
                  
                    fos = new FileOutputStream("workerID.backup");
                    oos = new ObjectOutputStream(fos);
                    oos.writeObject(workerID);
                    oos.close();
                    fos.close();
                }
                catch(IOException ioe) {
                      ioe.printStackTrace();
                }
            }
        });
        
        try { 
            String result[];
            String key, value;
            String response;
            
            String fromServer;
            socket = new Socket("127.0.0.1", 5555);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()) );
            
            try
            {
                File file = new File("hashmap.backup");
                File file2 = new File("workerID.backup");
                if(file.exists() && file2.exists()) {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        ObjectInputStream ois = new ObjectInputStream(fis);
                        keyValue = (HashMap) ois.readObject();
                        ois.close();
                    }
                    try (FileInputStream fis = new FileInputStream(file2)) {
                        ObjectInputStream ois = new ObjectInputStream(fis);
                        workerID = (int) ois.readObject();
                        ois.close();
                    }
                    out.println(workerID);
                }
                else {
                    out.println("**");
                    workerID = Integer.parseInt(in.readLine());
                }
            }catch(IOException ioe) {
               ioe.printStackTrace();
               return;
            }catch(ClassNotFoundException c) {
               System.out.println("Class not found");
               c.printStackTrace();
               return;
            }
            
            while ((fromServer = in.readLine()) != null) {
                workerThread workerthread = new workerThread();
                workerthread.start();
                
                if(fromServer.matches("^[_A-Za-z0-9-]+$")) {
                    key = fromServer;
                    response = workerthread.get(key);
                    if(response.equals("*")) {
                        out.println("Value does not exist!");
                    }
                    else {
                        out.println(response); // In this case response = value
                    }
                    
                }
                else if(fromServer.matches("^[_A-Za-z0-9-]+\\*[_A-Za-z0-9-]+$")) {
                    result = fromServer.split("\\*");
                    key = result[0];
                    value = result[1];
                    if(!( workerthread.get(key).equals("*") )) {
                        workerthread.put(key, value);
                        out.println(value + "*update");
                    }
                    else {
                        workerthread.put(key, value);
                        out.println(value + "*insert");
                    }
                    
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
