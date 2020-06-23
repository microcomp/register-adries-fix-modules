package ra;

import org.apache.commons.io.FileUtils;
import ra.ckan.ChangesManager;
import ra.ckan.ResourceManager;
import ra.ckan.ResourceInitializer;
import ra.transform.XmlToJsonTransformer;

import javax.json.JsonObject;
import javax.json.JsonString;
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;


/**
 * Class that puts together the entire process of repairing 'Register Adries'.
 * Runs in five steps described below.
 */
public class Main {
    private static final boolean PROD = true;
    /* all initialized in initializeResourceFoldersAndPaths() */
    private static String RESOURCES_DIRECTORY_PATH;
    public static String INIT_JSON_DIRECTORY_PATH;
    public static String INIT_XML_DIRECTORY_PATH;
    private static String CHANGES_DIRECTORY_PATH;
    private static String ZIP_DIRECTORY_PATH;
    private static String DOC_DIRECTORY_PATH;
    public static String DEBUG_FILE_PATH;

    //    static final String RESOURCE_IDS_FILE_PATH = "/resource_ids.txt";
    static final String RESOURCE_IDS_FILE_PATH = "/resource_ids_prod.txt";
    private static final String THIRD_DATASTORE_IDS_FILENAME = "init_changed_ids.txt";


    /* SETTINGS, used only for testing purposes */
    private static final boolean TEST = false;
    private static final String TEST_ORG_ID = "ministerstvo_vnutra_ra"; // used only for creating testing datasets
    //    private static final String TEST_ORG_ID = "datagov";
    // EMPTY STRING FOR PRODUCTION. since older ckan API cannot call purge_dataset I always had to alter the resourcename when testing
    private static final String TEST_SUFFIX = "";
    private static final boolean RUN_DELETE_TEST = false; // true if we only want to delete current testing datasets
    private static final boolean RUN_INIT_TEST = false; // true if we want to initialize datasets and their content for a new test


    /**
     * the main method, invoke this method to run the entire process
     */
    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
//        Registers registers =  new Registers(TEST);
        Registers registers =  new Registers();
        boolean uploadingChanges = false;

        if(args.length == 0 || args.length > 3) {
            throw new RuntimeException("1 or 2 arguments must be provided. See instructions for further details.");
        }
        /* patch for RA-muleprocess for 3rd datastore*/
        if (args[0].equalsIgnoreCase("-changes")) {
            if (args.length < 2) {
                throw new RuntimeException("Path to resource folder was not provided. See instructions for further details.");
            }
            RESOURCES_DIRECTORY_PATH = args[1];
            String INIT_CHANGED_IDS_PATH = RESOURCES_DIRECTORY_PATH + "init_changed_ids.txt";
            String apiKey = initializeApiKeyFromFile();
            CkanClient ckanClient = new CkanClient(apiKey, false);
            ChangesManager changesManager = new ChangesManager(ckanClient);
            initializeThirdIds(registers, INIT_CHANGED_IDS_PATH);
            CHANGES_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "changes/";
            File file = new File(CHANGES_DIRECTORY_PATH);
            if (!file.exists()) {
                if (!file.mkdir()) {
                    throw new RuntimeException("Unable to create directory to store the changes batches.");
                }
            }
            for (Registers.Register register : registers.getAllRegisters()) {
                System.out.println("Going to upload changes to init&changed datastore for register: " + register.getTitle());
                JsonObject resourceShow = ckanClient.callResourceShow(register.getThirdId());
                JsonString lastChangesId = resourceShow.getJsonObject("result").getJsonString("last_changes_id");
                if (lastChangesId == null) {
                    System.out.println("\nRESOURCE DOESN'T HAVE last_changes_id SET, SKIPPING.\n");
                    continue;
                }
                long lastApplied = Long.parseLong(lastChangesId.getString());
                changesManager.downloadChanges(lastApplied, CHANGES_DIRECTORY_PATH);
                changesManager.uploadChangesThirdDS(register, CHANGES_DIRECTORY_PATH);
                System.out.println("\nALL NECESSARY CHANGES BATCHES HAVE BEEN SUCCESSFULLY APPLIED.\n");
                // todo: highly inefficient to download them separately for each reg
                FileUtils.cleanDirectory(new File(CHANGES_DIRECTORY_PATH));
            }
            System.out.println("The process has finished successfully.");
            return;
        }


        RESOURCES_DIRECTORY_PATH = args[0];
        File resourceDir = new File(RESOURCES_DIRECTORY_PATH);
        if (!resourceDir.exists()){
            throw new RuntimeException("Directory with provided path doesn't exist. Please provide valid path and restart the program.");
        }

        long lastAppliedChangesId = 0L;
        if (args.length == 2) {
            String temp1 = args[1];
            try {
                lastAppliedChangesId = Long.parseLong(temp1);
            } catch(NumberFormatException e) {
                throw new RuntimeException("2nd argument must be a numerical value representing changes_id of the changes batch that has been" +
                        "applied to the initial batch as last OR 0 if you don't wish to apply any changes batches.");
            }
            if(lastAppliedChangesId < 0){
                throw new RuntimeException("2nd argument must be a positive value.");
            } else uploadingChanges = true;
        }


        initializeResourcesFoldersAndPaths(registers);
        String apiKey = initializeApiKeyFromFile();

        CkanClient ckanClient = new CkanClient(apiKey, false);

//        TestDataCreator test = new TestDataCreator(registers, ckanClient, TEST_ORG_ID, TEST_SUFFIX);
        ResourceManager resourceManager = new ResourceManager(ckanClient);
        ResourceInitializer resourceInit = new ResourceInitializer(ckanClient);
        ChangesManager changesManager = new ChangesManager(ckanClient);
        InitialTest tester = new InitialTest(ckanClient);

//        if(RUN_DELETE_TEST) {
//            test.runDelete(ZIP_DIRECTORY_PATH);
//            System.exit(0);
//        }
//
//        if (RUN_INIT_TEST) {
//            test.createTestingRegisters();
//            System.exit(0);
//        }

        initializeResourceIdsFromFile(registers, RESOURCE_IDS_FILE_PATH);

        tester.runInitialTest(RESOURCES_DIRECTORY_PATH, TEST_SUFFIX);

        // REGISTERS PRE-TEST
        System.out.println("RUNNING TESTS FOR REGISTERS IDs");
        for(Registers.Register register : registers.getAllRegisters()) {

            System.out.println("RUNNING TEST FOR REGISTER " + register.getName());
            JsonObject json = ckanClient.callResourceShow(register.getChangedId());
            if (json.getBoolean("success")) {
                boolean dsActive = json.getJsonObject("result").getBoolean("datastore_active");
                System.out.println(" \t"+register.getChangedId()+ " datastore active: "+ dsActive);
                if (!dsActive)
                    throw new RuntimeException("CKAN resource that should contain a datastore does not contain a datastore. Cannot proceed");
            } else {
                throw new RuntimeException(" \t"+register.getChangedId()+ " - ID NOT VALID");
            }
            json = ckanClient.callResourceShow(register.getConsolidatedId());
            if (json.getBoolean("success")) {
                boolean dsActive = json.getJsonObject("result").getBoolean("datastore_active");
                System.out.println(" \t"+register.getConsolidatedId()+ " datastore active: "+ dsActive);
                if (!dsActive)
                    throw new RuntimeException("CKAN resource that should contain a datastore does not contain a datastore. Cannot proceed");

            } else {
                throw new RuntimeException(" \t"+register.getConsolidatedId()+ " - ID NOT VALID");
            }
        }
        if (uploadingChanges) {
            System.out.println("Going to download all changes files that will be " +
                    "applied in the end of the process for each register.");
            changesManager.downloadChanges2018(CHANGES_DIRECTORY_PATH);
            changesManager.downloadChanges(lastAppliedChangesId, CHANGES_DIRECTORY_PATH);
            System.out.println("ALL NECESSARY CHANGES FILES HAVE BEEN SUCCESSFULLY STORED.");
        }

        for (Registers.Register register : registers.getAllRegisters()) {

            System.out.println("\nSTARTING THE CONSOLIDATION PROCESS FOR REGISTER *" + register.getResourceNameBase() + "*\n");

            /* STEP 1: Download and zip original resources. */
            resourceManager.downloadAndZip(register, ZIP_DIRECTORY_PATH,TEST_SUFFIX);
            System.out.println("\nALL RESOURCES HAVE BEEN DOWNLOADED AND ZIPPED SUCCESSFULLY\n");

            /* STEP 2: Delete datastore tables from changed and consolidated resources, entirely delete other resources in datasets. */
            resourceManager.clearDatasets(register, TEST_SUFFIX);
            System.out.println("\nALL DATASTORE TABLES HAVE BEEN REINITIALIZED AND OLD DOCUMENTATIONS AND INITIAL BATCHES DELETED\n");

            /* STEP 3: Upload new documentation (+ point the datastore resources to it) and new initial batches. */
            resourceManager.uploadDocsAndInitBatches(register, DOC_DIRECTORY_PATH, INIT_XML_DIRECTORY_PATH, TEST_SUFFIX);
//            resourceManager.uploadZipFiles(register, ZIP_DIRECTORY_PATH, TEST_SUFFIX);
            System.out.println("\nNEW DOCUMENTATIONS AND NEW INITIAL BATCHES HAVE BEEN SUCCESSFULLY UPLOADED.\n");

            /* STEP 4: Create new datastore tables in existing changed and consolidated resources and fill them with the JSON initial batches.*/
            resourceInit.createExtraDatastore(register);
            resourceInit.initializeDatastoresFromInitBatch(register, INIT_JSON_DIRECTORY_PATH);
            System.out.println("\nNEW DATASTORE TABLES HAVE BEEN CREATED AND INITIALIZED WITH INITIAL BATCHES.\n");

            /* STEP 5: Download all changes batches that have not yet been applied, upload them to datastore tables, then update the resources' metadata .*/
            if(uploadingChanges) {
                changesManager.uploadChanges(register, CHANGES_DIRECTORY_PATH);
                System.out.println("\nALL NECESSARY CHANGES BATCHES HAVE BEEN SUCCESSFULLY APPLIED.\n");
            }
        }
        /* delete temp files and prepare for the next use */
        System.out.println("going to delete temporary files created during the process and prepare for the next use.");
        FileUtils.cleanDirectory(new File(RESOURCES_DIRECTORY_PATH + "temp/"));

        // store new datastores' ids for convenience
        BufferedWriter br = new BufferedWriter(new FileWriter(RESOURCES_DIRECTORY_PATH + THIRD_DATASTORE_IDS_FILENAME));
        for (Registers.Register register : registers.getAllRegisters()) {
            br.write(register.getFileSuffix() + " " + register.getThirdId() + "\n");
        }
        br.close();

        Instant finish = Instant.now();
        long elapsedTime = Duration.between(start, finish).toMillis();
        long seconds = elapsedTime / 1000;
        long minutes = seconds / 60;
        System.out.println("\nTHE CONSOLIDATION PROCESS HAS FINISHED SUCCESSFULLY in " + minutes + " minutes and " +
                (seconds - minutes * 60) + " seconds.");
    }

    public static void initializeThirdIds(Registers registers, String idFilePath) throws IOException {
        File file = new File(idFilePath);
        if (!file.exists()) {
            throw new RuntimeException("Cannot find file with init & changed resource ids. Should be in: " + idFilePath);
        }
        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        ArrayList<String[]> ids = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            if (line.equals(""))
                continue;
            String[] parts = line.split(" ");

            for (Registers.Register register : registers.getAllRegisters()) {
                if (parts[0].equals(register.getFileSuffix())) {
                    register.setThirdId(parts[1]);
                    break;
                }
            }
        }
        for (Registers.Register register : registers.getAllRegisters()) {
            if (register.getThirdId() == null) {
                throw new RuntimeException("Changed & init id missing for at least 1 register.");
            }
        }

    }

    private static String initializeApiKeyFromFile() throws IOException {
        File keyFile = new File(RESOURCES_DIRECTORY_PATH + "apikey.txt");
        String apiKey;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(keyFile));
            apiKey = reader.readLine();
            System.out.println("API KEY: " + apiKey);
            reader.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to find file " + RESOURCES_DIRECTORY_PATH + "apikey.txt with the authorization" +
                    " key to CKAN API for data.gov.sk. Please create the file and provide a valid authorization key.");
        }
        return apiKey;
    }

    /* checks if all of the needed resources are present in their specified directories, initializes static fields used across
     * the program that represent paths to individual resource folders and creates the temp/  */
    private static void initializeResourcesFoldersAndPaths(Registers registers) {
        /* init batch */
        INIT_JSON_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "init_batch/json/";
        File initJson = new File(INIT_JSON_DIRECTORY_PATH);
        if (!initJson.exists()){
            throw new RuntimeException("Unable to find the directory with the JSON initial batch " + INIT_JSON_DIRECTORY_PATH + ". Make sure the " +
                    "transformation process has been executed properly.");
        }
        File[] initJsonFiles = initJson.listFiles();
        if(initJsonFiles == null || initJsonFiles.length < registers.getAllRegisters().size()){
            throw new RuntimeException("The directory that is supposed to contain the JSON initial batches is missing files. Make sure" +
                    " the transformation process has been executed properly.");
        }

        INIT_XML_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "init_batch/xml/";
        File initXml = new File(INIT_XML_DIRECTORY_PATH);
        if (!initXml.exists()){
            throw new RuntimeException("Unable to find the directory with the XML initial batch (...init_batch/xml/). " +
                    "Make sure it's there and that it contains the XML initial batches for all registers.");
        }
        checkDirectoryContents(registers, initXml, "xml");

        /* documentations */
        DOC_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "doc/";
        File doc = new File(DOC_DIRECTORY_PATH);
        if(!doc.exists()){
            throw new RuntimeException("Unable to find the documentation directory " + DOC_DIRECTORY_PATH + ". Make sure that " +
                    "it's created and contains the documentation .html files for all registers.");
        }
        checkDirectoryContents(registers, doc, "html");

        /* temp files */
        CHANGES_DIRECTORY_PATH = RESOURCES_DIRECTORY_PATH + "temp/changes/";
        ZIP_DIRECTORY_PATH =  RESOURCES_DIRECTORY_PATH + "backup/";
        File changes = new File(CHANGES_DIRECTORY_PATH);
        File zip = new File(ZIP_DIRECTORY_PATH);
        if(!changes.exists()) {
            if (!changes.mkdirs()) {
                throw new RuntimeException("Failed to create folder for changes batches (" + changes.getPath() + ").");
            }
        }

        if(!zip.exists()){
            if(!zip.mkdirs()){
                throw new RuntimeException("Failed to create folder for zip archives (" + zip.getPath() + ").");
            }
        }
        DEBUG_FILE_PATH = RESOURCES_DIRECTORY_PATH + "error.json";
    }

    private static void checkDirectoryContents(Registers registers, File dir, String format) {
        File[] files = dir.listFiles();
        if(files == null || files.length < registers.getAllRegisters().size()){
            throw new RuntimeException("The directory " + dir.getName() + " that is supposed to contain resource " +
                    "files is missing files. Make sure that it contains the correct resources for all registers. " +
                    "For more information see the instructions.");
        }
        for(Registers.Register register : registers.getAllRegisters()){
            boolean found = false;
            String suffix = "-" + register.getFileSuffix() + "." + format;
            for(File file : files){
                if(file.getName().endsWith(suffix)){
                    found = true;
                    if(format.equalsIgnoreCase("xml")) register.setXmlFileName(file.getName());
                    else if(format.equalsIgnoreCase("html")) register.setDocFileName(file.getName());
                }
            }
            if(!found) {
                throw new RuntimeException("Unable to find resource file in " + dir.getPath() + " for at least 1 register." +
                        " Please make sure all of the necessary resource files are provided and follow the naming convention " +
                        "(for more information see the instructions). ");
            }
        }
    }

    public static void initializeResourceIdsFromFile(Registers registers, String idFilePath) throws IOException {
        InputStream is = Main.class.getResourceAsStream(idFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        String[] parts;

        while ((line = br.readLine()) != null) {
            if ("".equals(line)) continue;
            parts = line.split(" ");
            if (parts.length != 3) {
                throw new RuntimeException("File that contains resource ids for Register Adries resources" +
                        " has invalid formatting.");
            }
            for (Registers.Register register : registers.getAllRegisters()) {
                if (register.getFileSuffix().equals(parts[0])) {
                    register.setChangedId(parts[1]);
                    register.setConsolidatedId(parts[2]);
                    break;
                }
            }
        }
        br.close();
        for (Registers.Register register : registers.getAllRegisters()) {
            if (register.getChangedId() == null) {
                throw new RuntimeException("Resource ids missing from file for at least 1 initialized register.");
            }
        }
    }
}
