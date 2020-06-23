package ra.additional_fixes;

import org.apache.commons.io.FileUtils;
import ra.CkanClient;
import ra.InitialTest;
import ra.Registers;
import ra.ckan.ChangesManager;
import ra.ckan.ResourceManager;
import ra.ckan.ResourceInitializer;
import ra.transform.XmlToJsonTransformer;

import javax.json.*;
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


/**
 * Class that puts together the entire process of repairing 'Register Adries'.
 * Runs in five steps described below.
 */
public class Fix2 {
    private static final boolean PROD = true;

    //    static final String RESOURCE_IDS_FILE_PATH = "/resource_ids.txt";
    static final String RESOURCE_IDS_FILE_PATH = "/all_ids_prod.txt";
    public static String DEBUG_FILE_PATH;
    public static String INIT_JSON_DIRECTORY_PATH = "";
    private static final String TEST_SUFFIX = "";


    /**
     * the main method, invoke this method to run the entire process
     */
    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
//        Registers registers =  new Registers(TEST);
        Registers registers =  new Registers();

        if (args.length != 1 && args.length != 2) {
            throw new RuntimeException("1 or 2 arguments with the path to the directory containing CKAN API key (under apikey.txt) must be provided.");
        }

        boolean dryRun = false;
        String basePath;
        if (args.length == 2) {
            if (!args[0].equals("-test")) {
                throw new RuntimeException("Invalid first argument.");
            }
            System.out.println("Running the app in a test mode, no changes will be applied to CKAN.");
            dryRun = true;
            basePath = args[1];

        } else {
            basePath = args[0];
        }


        File BASE_DIR = new File(basePath);

        if (!BASE_DIR.exists()) {
            throw new RuntimeException("Directory with provided path " + basePath + " doesn't exist. Cannot proceed.");
        }
        DEBUG_FILE_PATH = basePath + "debug.json";
        String apiKeyPath = basePath + "apikey.txt";

        String apiKey = initializeApiKeyFromFile(apiKeyPath);

        String changesDirectoryPath = basePath + "changes_missing/";

        File CHANGES_DIR = new File(changesDirectoryPath);
        if (CHANGES_DIR.exists()) {
            FileUtils.deleteQuietly(CHANGES_DIR);
        }
        if (!CHANGES_DIR.mkdir()) {
            throw new RuntimeException("Unable to create directory " + changesDirectoryPath + ". Cannot proceed.");
        }

        CkanClient ckanClient = new CkanClient(apiKey, false);

//        TestDataCreator test = new TestDataCreator(registers, ckanClient, TEST_ORG_ID, TEST_SUFFIX);
        ResourceManager resourceManager = new ResourceManager(ckanClient);
        ResourceInitializer resourceInit = new ResourceInitializer(ckanClient);
        ChangesManager changesManager = new ChangesManager(ckanClient);
        InitialTest tester = new InitialTest(ckanClient);

        initializeAllResourceIdsFromFile(registers, RESOURCE_IDS_FILE_PATH);

//        // REGISTERS PRE-TEST
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

            json = ckanClient.callResourceShow(register.getThirdId());
            if (json.getBoolean("success")) {
                boolean dsActive = json.getJsonObject("result").getBoolean("datastore_active");
                System.out.println(" \t"+register.getThirdId()+ " datastore active: "+ dsActive);
                if (!dsActive)
                    throw new RuntimeException("CKAN resource that should contain a datastore does not contain a datastore. Cannot proceed");

            } else {
                throw new RuntimeException(" \t"+register.getThirdId()+ " - ID NOT VALID");
            }
        }

        System.out.println("Going to download and unzip changes.");
        changesManager.downloadChanges2018(changesDirectoryPath);

        long MISSING_FROM = 8835586;

        List<File> xmlResources = ChangesManager.loadXmlResources(changesDirectoryPath);
        for (Registers.Register register : registers.getAllRegisters()) {
            System.out.println("\nGoing to upload missing changes for resigter *" + register.getResourceNameBase() + "*\n");
            boolean changeExists = false;
            XmlToJsonTransformer xmlToJsonTransformer = new XmlToJsonTransformer();

            for (File xmlResource : xmlResources) {

                String changesIdStr = ChangesManager.parseChangesId(xmlResource.getName());

                long changesId = Long.parseLong(changesIdStr);
                if (changesId <= MISSING_FROM) {
                    continue;
                }

                JsonArray jsonRecordsInsert = xmlToJsonTransformer.transformXmlToJson(xmlResource, register, "CHANGES");
                if (jsonRecordsInsert.size() > 0) {
                    changeExists = true;
                    System.out.println("\nApplying Zmenova davka c. " + changesIdStr + " to register " + register.getResourceNameBase());
                    System.out.println("Following records will be inserted to the changes and init+changes resources:");
                    for (JsonObject record : jsonRecordsInsert.getValuesAs(JsonObject.class)) {
                        System.out.println("objectId: " + record.getString("objectId") + " versionId: " +
                                record.getString("versionId"));
                    }
                }
//                System.out.println("Inserting missing records to changes and init+changes resource.");
                if (!dryRun && (changesId != 8837823L || register.getRegisterType() != XmlToJsonTransformer.RegisterType.STREET_NAME)) {
                    String response = ckanClient.updateDatastore(register.getChangedId(), jsonRecordsInsert, "insert");
                    String response2 = ckanClient.updateDatastore(register.getThirdId(), jsonRecordsInsert, "insert");
                }

                JsonArray jsonRecordsUpsert = xmlToJsonTransformer.transformXmlToJson(xmlResource, register, "CONSOLIDATED_UPSERT");
                JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());

                JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
//                System.out.println("Going to filter out outdated records for consolidated resource.");
                for (JsonObject record : jsonRecordsUpsert.getValuesAs(JsonObject.class)) {
                    String versionId = record.getString("versionId");
                    String recordChangeId = record.getString("changeId");
                    JsonObject res = ckanClient.searchDatastore(register.getConsolidatedId(),
                            new HashMap<String, String>() {{ put("versionId", versionId); }});
                    JsonArray matchingRecords = res.getJsonObject("result").getJsonArray("records");
                    boolean skip = false;
                    for (JsonObject match : matchingRecords.getValuesAs(JsonObject.class)) {
                        JsonValue jsonChangeId = match.get("changeId");
//                        JsonString jsonChangeId = match.getJsonString("changeId");


                        if (jsonChangeId != null) {
                            String changeIdStr = jsonChangeId.toString();
                            changeIdStr = changeIdStr.replace("\"", "");
                            if (!changeIdStr.equals("") && !changeIdStr.equals("null")) {
                                long changeIdCkan = Long.parseLong(changeIdStr);
                                //                            long changeIdCkan = jsonChangeId.longValue();
//                                System.out.println("ckan: " + changeIdCkan + "  change: " + recordChangeId);
                                if (changeIdCkan >= changesId) {
                                    // skipping (would be replaced)
                                    skip = true;
//                                    System.out.println("Skipping outdated record");
                                    break;
                                }
                            }
                        }
                    }
                    if (!skip) {
                        arrayBuilder.add(record);
                    }
                }
                JsonArray filteredRecords = arrayBuilder.build();
                if (filteredRecords.size() > 0) {
                    System.out.println("Following records will be upserted to the consolidated resource: ");
                    for (JsonObject record : filteredRecords.getValuesAs(JsonObject.class)) {
                        System.out.println("objectId: " + record.getString("objectId") + " versionId: " +
                                record.getString("versionId"));
                    }
                }

//                System.out.println(filteredRecords.size() + "/" + jsonRecordsUpsert.size() + " records remaining to be inserted." );
//                System.out.println("Inserting missing records to consolidated resource.");
                if (!dryRun) {
                    String response3 = ckanClient.updateDatastore(register.getConsolidatedId(), filteredRecords, "upsert");
                }
                // updating consolidated resource's metadata (last_changes_id and last_update_successful)
            }
            if (!changeExists) {
                System.out.println("No changes are needed to be applied.");
            }
        }
        /* delete temp files and prepare for the next use */
        FileUtils.deleteQuietly(CHANGES_DIR);
        Instant finish = Instant.now();
        long elapsedTime = Duration.between(start, finish).toMillis();
        long seconds = elapsedTime / 1000;
        long minutes = seconds / 60;
        System.out.println("\nTHE CONSOLIDATION PROCESS HAS FINISHED SUCCESSFULLY in " + minutes + " minutes and " +
                (seconds - minutes * 60) + " seconds.");
    }

    private static String initializeApiKeyFromFile(String path) throws IOException {
        File keyFile = new File(path);
        String apiKey;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(keyFile));
            apiKey = reader.readLine();
            System.out.println("API KEY: " + apiKey);
            reader.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to find file " + path + " with the authorization" +
                    " key to CKAN API for data.gov.sk. Please create the file and provide a valid authorization key on a single line.");
        }

        return apiKey;
    }

    public static void initializeAllResourceIdsFromFile(Registers registers, String idFilePath) throws IOException {
        InputStream is = Fix2.class.getResourceAsStream(idFilePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        String[] parts;

        while ((line = br.readLine()) != null) {
            if ("".equals(line)) continue;
            parts = line.split(" ");
            if (parts.length != 4) {
                throw new RuntimeException("File that contains resource ids for Register Adries resources" +
                        " has invalid formatting.");
            }
            for (Registers.Register register : registers.getAllRegisters()) {
                if (register.getFileSuffix().equals(parts[0])) {
                    register.setChangedId(parts[1]);
                    register.setConsolidatedId(parts[2]);
                    register.setThirdId(parts[3]);
                    break;
                }
            }
        }
        br.close();
        for (Registers.Register register : registers.getAllRegisters()) {
            if (register.getChangedId() == null || register.getConsolidatedId() == null || register.getThirdId() == null) {
                throw new RuntimeException("Resource ids missing from file for at least 1 initialized register.");
            }
        }
    }
}
