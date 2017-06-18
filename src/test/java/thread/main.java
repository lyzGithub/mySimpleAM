package thread;

import java.io.File;

/**
 * Created by ubuntu2 on 6/17/17.
 */
public class main {
    public static void main(String[] args)throws Exception{
        File file = new File("helloAllocate.txt");
        if(!file.exists())
            file.createNewFile();
    }
}
