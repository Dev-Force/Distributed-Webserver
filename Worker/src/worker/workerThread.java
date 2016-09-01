package worker;

import java.util.logging.Level;
import java.util.logging.Logger;


class workerThread extends Thread{
    
    public String get(String key) {
        try {
            System.out.println(Worker.keyValue.get(key).toString());
            return Worker.keyValue.get(key).toString();
        }
        catch(NullPointerException e) {
            return "*";
        }
    }
    
    public void put(String key, String value){
            Worker.keyValue.put(key, value);
        // }
    }
    
} // End worketThread
