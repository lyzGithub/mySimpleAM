package justForTes;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ubuntu2 on 6/24/17.
 */
public class testClass {
    public  static void main(String []args){
        Map<String,String> m1 = new HashMap<String, String>();
        m1.put("t","t");
        Map<String,String> m2 = new HashMap<String, String>(m1);
        System.out.println("m1"+ m1.containsKey("t")+":" + "m2"+m2.containsKey("t"));
        m1.clear();
        System.out.println("m1"+ m1.containsKey("t")+":" + "m2"+m2.containsKey("t"));
    }
}
