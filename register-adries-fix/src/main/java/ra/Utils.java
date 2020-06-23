package ra;

import org.apache.http.HttpResponse;
import ra.additional_fixes.Main;

import java.io.*;

/**
 * Contains static methods user across the program.
 */
public class Utils {
    private Utils() {
    }


    public static void jsonStringToFile(String jsonString) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(ra.additional_fixes.Main.DEBUG_FILE_PATH));
        writer.write(jsonString);
        writer.close();
    }
    /* only used for zip file names */
    public static String removeDiacritics(String string) {
        string = string.replace("á", "a")
                .replace("š","s").replace("č","c")
                .replace("Č","C").replace("ť","t")
                .replace("ž","z").replace("í","i")
                .replace("é","e").replace("ú","u");
        return string;
    }

    public static void ckanResponseToFile(HttpResponse response) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(ra.additional_fixes.Main.DEBUG_FILE_PATH));
            writer.write(convertStreamToString(response.getEntity().getContent()));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Ckan error response has been writen to file " + Main.DEBUG_FILE_PATH +".");
    }

    private static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    public static void storeResourceIdsToFile(Registers registers, String filePath) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(new File(filePath), "UTF-8");
        for(Registers.Register register : registers.getAllRegisters()){
            writer.print(register.getFileSuffix() + " " + register.getChangedId() + " " + register.getConsolidatedId() + "\n");
        }
        writer.close();
    }


    private static void addXmlMissingTag(String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
        writer.write("</register>");
        writer.close();
    }

    private static void test(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        int i = 0;
        String line;
        while((line = reader.readLine()) != null){
            i++;
            if(line.contains("register")){
                System.out.println(line);
            }
        }
        System.out.println(i);
    }
}
