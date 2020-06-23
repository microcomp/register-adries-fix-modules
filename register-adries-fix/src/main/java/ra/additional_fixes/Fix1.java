package ra.additional_fixes;


import ra.CkanClient;
import ra.Main;
import ra.Registers;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.io.File;
import java.io.IOException;

public class Fix1 {
//
    public static void main(String[] args) throws Exception {
//        ChangesManager ch = new ChangesManager(new CkanClient("", false));
//        ch.downloadChanges2018("/home/boris/Desktop/testt/");
    }


    private static void updateDocs() throws IOException {
        CkanClient ck = new CkanClient("17e26bd0-cded-4995-afbc-75a63ead21e6", true);
        Registers regs = new Registers();
        Main.DEBUG_FILE_PATH = "/home/boris/Desktop/error.json";

        for (Registers.Register register : regs.getAllRegisters()) {
            File upload = new File("/home/boris/Desktop/microcomp_2020/doc/doc-" + register.getFileSuffix() + ".html");
            JsonObject packageShow = ck.callPackageShow(register.getName());
            JsonArray resources = packageShow.getJsonObject("result").getJsonArray("resources");
            for (int i = 0; i < resources.size(); i++) {
//            System.out.println(i + " \\ " + resources.size());
                JsonObject resource = resources.getJsonObject(i);
                JsonString format = resource.getJsonString("format");
                if (format != null && "HTML".equalsIgnoreCase(format.getString())) {
                    ck.updateResourceFile(resource.getString("id"), upload);
                    System.out.println("** Updated register: " + register.getTitle());
                    break;
                }
            }

        }

    }
}
//        ck.deleteDatastore("54ab3521-577c-4dbd-ae60-857fc51bb914");
//        ck.deleteResource("54ab3521-577c-4dbd-ae60-857fc51bb914");
//        ck.deleteResource("9e938ceb-ff51-49dc-b3cf-f85a9d88a7cf");
//        ck.deleteResource("bc84c409-1c23-4bb8-b2d8-70b0ee568bf0");
//        for (Registers.Register register : regs.getAllRegisters()) {
//        }


//        Registers regs = new Registers(true);
//        ResourceInitializer ri = new ResourceInitializer(ck);
//        for (Registers.Register register : regs.getAllRegisters()) {
//            ri.createExtraDatastore(register);
//            System.out.println("created");
//            ck.createDatastore(register.getThirdId(), register.getChangedTableFields(), new String[]{});
//        }

//    }
//    public static void main(String[] args) throws Exception {
//        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date());
//        System.out.println(modifiedTimestamp);
//        CkanClient ck = new CkanClient("17e26bd0-cded-4995-afbc-75a63ead21e6", true);
//
//        ChangesManager cm = new ChangesManager(ck);
////        cm.downloadChanges(575629, "/home/boris/Desktop/microcomp_2020/temp/");
//
//        List<File> xmlResources = ChangesManager.loadXmlResources("/home/boris/Desktop/microcomp_2020/temp/");
//        XmlToJsonTransformer transformer = new XmlToJsonTransformer();
//
//        Registers registers = new Registers();
//        for(Registers.Register register : registers.getAllRegisters()){
//            for (File xmlResource : xmlResources) {
//                String changesId = ChangesManager.parseChangesId(xmlResource.getName());
//                System.out.println("Applying Zmenova davka c. " + changesId + " to register " + register.getResourceNameBase());
//                JsonArray jsonRecordsInsert = transformer.transformXmlToJson(xmlResource, register, "CHANGES");
//            }
//        }
//    }
//}



//
//
//public class Temp {
//    public static void main(String[] args) throws IOException {
////        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).format(new Date());
////        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date("2020-04-07 10:16:28"));
//        System.out.println(new Date());
//
//
////        CkanClient ck = new CkanClient("17e26bd0-cded-4995-afbc-75a63ead21e6", true);
////        Registers regs = new Registers(true);
////        ResourceInitializer ri = new ResourceInitializer(ck);
////        for (Registers.Register register : regs.getAllRegisters()) {
////            ri.createExtraDatastore(register);
////            System.out.println("created");
////            ck.createDatastore(register.getThirdId(), register.getChangedTableFields(), new String[] {} );
////
////        }
//    }
//}


















//public class Temp {
//    private static final String API_KEY_FIX = "17e26bd0-cded-4995-afbc-75a63ead21e6";
//    private static final String RESOURCE_IDS_FILE_PATH = "/resource_ids.txt";
//    private static final String TEST_ID = "33ffb8d4-91b1-43f9-8304-af4dd1e2d71e";
//    public static void main(String[] args) throws Exception {
//        Registers registers = new Registers();
//        CkanClient ck = new CkanClient(API_KEY_FIX, false);
//        Main.DEBUG_FILE_PATH = "/home/boris/Desktop/error.json";
//        Main.initializeResourceIdsFromFile(registers, RESOURCE_IDS_FILE_PATH);
//        RequestBuilder rb = new RequestBuilder();
//        ChangesManager ch = new ChangesManager(ck);
//        XmlToJsonTransformer tr = new XmlToJsonTransformer();
//        for (Registers.Register register : registers.getAllRegisters()) {
//            if (register.getRegisterType() != XmlToJsonTransformer.RegisterType.STREET_NAME)
//                continue;
////            System.out.println(register.getName());
////            JsonObject response = ck.callPackageShow(register.getName());
//            System.out.println(register.getName());
//
//            String changedName = register.getResourceNameBase() + " - test";
//            String packageId = register.getName();
//
////            JsonObject response = ck.createResource(changedName, packageId, "daily", "csv");
////            String changedId = response.getJsonObject("result").getString("id");
////            ck.createDatastore("33ffb8d4-91b1-43f9-8304-af4dd1e2d71e", register.getChangedTableFields(), new String[] {} );
////            System.out.println("Inserting init batch");
////            File file = new File("/home/boris/Desktop/microcomp_2020/init_batch/json/STREET_NAME/0.json");
////            JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.emptyMap());
////            JsonReader jsonReader = readerFactory.createReader(new FileInputStream(file), Charsets.UTF_8);
////            JsonArray jsonRecords = jsonReader.readArray();
////            ck.updateDatastore("33ffb8d4-91b1-43f9-8304-af4dd1e2d71e", jsonRecords, "insert");
//
////            ch.downloadChanges(575629, "/home/boris/Desktop/temp/");
////                ch.uploadChanges(register,"/home/boris/Desktop/temp/" );
//            List<File> xmlResources = loadXmlResources("/home/boris/Desktop/temp/");
//            for (File xmlResource : xmlResources) {
//                String changesId = parseChangesId(xmlResource.getName());
//                System.out.println("Applying Zmenova davka c. " + changesId + " to register " + register.getResourceNameBase());
//                JsonArray jsonRecordsInsert = tr.transformXmlToJson(xmlResource, register, "CHANGES");
//                String response = ck.updateDatastore(TEST_ID, jsonRecordsInsert, "insert");
//                // updating changed resource's metadata (last_changes_id)
//
//            }
//
////            ck.createDatastore(register.getConsolidatedId(), register.getConsolidatedTableFields(), register.getPrimaryKeys());
//
//
////            HttpEntity entity = new StringEntity(rb.buildJsonHttpRequest(params), Charsets.UTF_8);
////            JsonObject response = ck.executeHttpPostRequest(entity, "datastore_search");
//
//        }
//
//    }
//
//
//    private static List<File> loadXmlResources(String path) {
//        File sourceDirectory = new File(path);
//
//        File[] changesFiles = sourceDirectory.listFiles();
//        if (changesFiles == null || changesFiles.length < 1) {
//            System.out.println("Changes folder is empty, so no new changes batches are going to be applied.");
//            return Collections.emptyList();
//        }
//        ArrayList<File> resources =  new ArrayList<>(Arrays.asList(changesFiles));
//        resources.sort((o1, o2) -> {
//            int changesId1 = Integer.parseInt(parseChangesId((o1).getName()));
//            int changesId2 = Integer.parseInt(parseChangesId((o2).getName()));
//            return  changesId1 - changesId2;
//        });
//        return resources;
//    }
//
//    private static String parseChangesId(String fileName) {
//        Pattern pattern = Pattern.compile("[a-z]+(\\d+)(.xml)");
//        Matcher matcher = pattern.matcher(fileName);
//        if (matcher.matches()){
//            return matcher.group(1);
//        }
//        throw new RuntimeException("Unable to parse changes_id from file name " + fileName + ". Make sure" +
//                "it has the proper format zmenovadavkaX.xml where X stands for changes_id of the particular" +
//                "changes file.");
//    }
//}
//
//
///*
//* ic void main(String[] args) throws IOException {
//        Registers registers = new Registers();
//        CkanClient ck = new CkanClient(API_KEY_FIX, true);
//        Main.DEBUG_FILE_PATH = "/home/boris/Desktop/error.json";
//        Main.initializeResourceIdsFromFile(registers, RESOURCE_IDS_FILE_PATH);
//        RequestBuilder rb = new RequestBuilder();
//        int j = 0;
//        for (Registers.Register register : registers.getAllRegisters()) {
//            if (register.getRegisterType() == XmlToJsonTransformer.RegisterType.STREET_NAME)
//                continue;
////            System.out.println(register.getName());
////            JsonObject response = ck.callPackageShow(register.getName());
//            System.out.println(register.getName());
//            Map<String, String> params = new HashMap<>();
//            params.put("resource_id", register.getConsolidatedId());
//            params.put("fields", "versionId");
//            params.put("limit", String.valueOf(1000000));
//
//            HttpEntity entity = new StringEntity(rb.buildJsonHttpRequest(params), Charsets.UTF_8);
//            int i = 0;
//            JsonObject response = ck.executeHttpPostRequest(entity, "datastore_search");
//
//            HashSet<Integer> seen = new HashSet<>();
//            for (JsonObject entry : response.getJsonObject("result").getJsonArray("records").getValuesAs(JsonObject.class)) {
//                i++;
//                Integer vId = Integer.parseInt(entry.getString("versionId"));
//
//                if (seen.contains(vId)) {
//                    throw new RuntimeException("duplicate");
//                }
//                seen.add(vId);
//
//            }
//            System.out.println(i);
//            if (j == 5)
//                break;
//            j++;
////            HashMap<String, String> params = new HashMap<>();
////            params.put("url_type", "datastore");
////            ck.updateResource(register.getConsolidatedId(), params);
//        }
//
//    }*/