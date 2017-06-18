package thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by ubuntu2 on 6/16/17.
 */
public class testThread1 {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public Future<String> getData() {
        return executor.submit(new Callable<String>() {

            public String call() throws Exception {
                System.out.println("my data!");
                return "hello world!";
            }
        });
    }
}