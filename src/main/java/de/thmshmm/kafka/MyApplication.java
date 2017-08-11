package de.thmshmm.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Thomas Hamm on 24.07.17.
 */
public class MyApplication {
    public static void main(String[] args) {
        Properties props = new Properties();

        InputStream is = null;

        try {
            is = new FileInputStream("client.properties");
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        MyKafkaConsumer myConsumer = new MyKafkaConsumer(props);

        Thread t = new Thread(myConsumer);
        t.start();
    }
}
