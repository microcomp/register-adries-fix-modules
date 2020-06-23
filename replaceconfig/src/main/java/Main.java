import org.apache.commons.io.FileUtils;

import java.io.*;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new RuntimeException("Please provide path to the root directory of the ra_ws_muleprocess project. " +
                    "(the path should be something similar to this: /usr/share/mule/apps/ra_ws_muleprocess/");
        }

        String path = args[0];

        File dir = new File(path);
        if(!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("Directory with given path doesn't exist or is not a directory, please provide a valid path." +
                    "(the path should be something similar to this: /usr/share/mule/apps/ra_ws_muleprocess/)");
        }

        path += "classes/transform/xml_to_json_transform_config.csv";

        File oldFile = new File(path);
        if(!oldFile.exists()) {
            throw new RuntimeException("Unable to locate file " + oldFile.getPath() + ". Please make sure that you provided a valid" +
                    " path to the root directory of the ra_ws_muleprocess project (the path should be something similar to this: /usr/share/mule/apps/ra_ws_muleprocess/).");
        }
        InputStream is = Main.class.getResourceAsStream("/xml_to_json_transform_config.csv");

        System.out.println("Going to make necessary changes of the ra_ws_muleprocess project files.");
        FileUtils.copyInputStreamToFile(is, oldFile);

        System.out.println("DONE");
    }
}
