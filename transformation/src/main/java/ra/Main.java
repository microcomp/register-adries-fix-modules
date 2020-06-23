package ra;

import ra.Main;
import ra.Registers;
import ra.transform.XmlToJsonTransformer;

import javax.json.JsonArray;
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class is used to convert the initial batches from .xml to .json format.
 * Execute the main() method in order to perform the transformation.
 * Assumes that the XML files are in "http://www.minv.sk/ra" namespace
 */
public class Main {

    private static String RESOURCES_DIRECTORY_PATH;
    public static String INIT_JSON_DIRECTORY_PATH;
    private static String INIT_XML_DIRECTORY_PATH;
    public static String XML_NAMESPACE = "http://www.minv.sk/ra";

    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
        Registers registers = new Registers();

        if(args.length != 1) {
            throw new RuntimeException("Exactly 1 argument must be provided. See instructions for further details.");
        }
        RESOURCES_DIRECTORY_PATH = args[0];
        File resourceDir = new File(RESOURCES_DIRECTORY_PATH);
        if (!resourceDir.exists()) {
            throw new RuntimeException("Directory with provided path doesn't exist. Please provide valid path and restart the program.");
        }
        INIT_XML_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "init_batch/xml/";
        File initXml = new File(INIT_XML_DIRECTORY_PATH);
        if (!initXml.exists()){
            throw new RuntimeException("Unable to find the directory with the XML initial batches ( " + INIT_XML_DIRECTORY_PATH + "). " +
                    "Make sure that it's there and that it contains valid XML initial batches for all registers.");
        }
        checkDirectoryContents(registers, initXml);

        INIT_JSON_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "init_batch/json/";
        File initJson = new File(INIT_JSON_DIRECTORY_PATH);
        if (!initJson.exists()){
            if(!initJson.mkdir()) {
                throw new RuntimeException("Failed to create json/ destination directory for the transformation.");
            }
        } else {
            File[] content = initJson.listFiles();
            if(content != null){
                if ( content.length > 0 ){
                    throw new RuntimeException("The destination json/ directory contains files. Make sure it's empty before the transformation begins.");
                }
            }
        }
//        File dir = new File(INIT_XML_DIRECTORY_PATH + "old/");
//        removeNameSpacesAndInvalidTagsFromXml(Arrays.asList(Objects.requireNonNull(dir.listFiles())));
        XmlToJsonTransformer transformer = new XmlToJsonTransformer();
        System.out.println("THE TRANSFORMATION PROCESS IS GOING TO BEGIN.");
        for(Registers.Register register : registers.getAllRegisters()){
            File initBatch = new File(INIT_XML_DIRECTORY_PATH + register.getXmlFileName());
            transformer.transformXmlToJson(initBatch, register);
        }

        Instant finish = Instant.now();
        long elapsedTime = Duration.between(start, finish).toMillis();
        long seconds = elapsedTime / 1000;
        long minutes = seconds / 60;
        System.out.println("\nTHE TRANSFORMATION PROCESS HAS FINISHED SUCCESSFULLY in " + minutes + " minutes and " +
                (seconds - minutes * 60) + " seconds.");
    }

    /**
     * checks if the XML source directory contains all necessary resources that follow naming conventions described
     * in the program manual. sets the init batches' file names for all registers
     */
    private static void checkDirectoryContents(Registers registers, File dir) {
        File[] files = dir.listFiles();
        if(files == null || files.length < registers.getAllRegisters().size()){
            throw new RuntimeException("The directory " + dir.getName() + " that is supposed to contain resource " +
                    "files is missing files. Make sure that it contains the correct resources for all registers. " +
                    "For more information see the instructions.");
        }

        for(Registers.Register register : registers.getAllRegisters()){
            boolean found = false;
            String suffix = "-" + register.getFileSuffix() + ".xml";
            for(File file : files){
                if(file.getName().endsWith(suffix)){
                    found = true;
                    register.setXmlFileName(file.getName());
                }
            }
            if(!found) {
                throw new RuntimeException("Unable to find resource file in " + dir.getPath() + " for at least 1 register." +
                        " Please make sure all of the necessary resource files are provided and follow the naming convention " +
                        "(for more information see the instructions). ");
            }
        }
    }

    private static void changeCsvDelimiter(String csvFilePath, Character from, Character to) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(csvFilePath)));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(csvFilePath.replace(".csv", "_.csv"))));

        String line;
        while((line = br.readLine()) != null) {
            line = line.replace(from, to);
            bw.write(line + "\n");
        }
        br.close();
        bw.close();
    }


//    /**
//     * some of the XML init contain strange namespaces in the first tag, these have to be removed in order for transformer to work
//     * also some of the batches were contained invalid tags that had to be removed
//     */
    private static void removeNameSpacesAndInvalidTagsFromXml(List<File> files) throws IOException {
        for(File file : files) {
            if (file.isDirectory())
                continue;
            boolean deletingTags = file.getName().endsWith("07.xml") || file.getName().endsWith("06.xml");
            System.out.println("Moving on to " + file.getName());
            BufferedReader reader = new BufferedReader(new FileReader(file));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(INIT_XML_DIRECTORY_PATH + file.getName())));
            int i = 0;
            String line;
            while((line = reader.readLine()) != null){
                i++;
                if (i == 2) {
                    line = line.replace(":ns0", "");
                    line = line.replace("ns0:", "");

//                    if (line.contains(" xmlns:ns0=\"http://www.minv.sk/ra\"")){
////                        line = line.replace("ns0:", "").replace(" xmlns:ns0=\"http://www.minv.sk/ra\"", "");
//                    }
//                    else {
//                        line = line.replace(" xmlns=\"http://www.minv.sk/ra\"", "");
//                    }
                }
                if(line.contains("ns0:")) line = line.replace("ns0:", "");
                if(deletingTags && line.contains("</register>")) continue;
                writer.write(line + "\n");
            }
            if (deletingTags)
                writer.write("</register>" + "\n");
            writer.close();
            reader.close();
        }
    }

}


