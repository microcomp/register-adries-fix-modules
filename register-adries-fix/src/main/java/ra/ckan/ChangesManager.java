package ra.ckan;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import ra.CkanClient;
import ra.Registers;
import ra.transform.XmlToJsonTransformer;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Handles all operations concerning changes batches from dataset "register-adries-ra-zmenove-davky".
 */
public class ChangesManager {
    private static final String CHANGES_PACKAGE_ID = "register-adries-ra-zmenove-davky"; // PROD
    //    private static final String CHANGES_PACKAGE_ID = "register-adries-zmenove-davky"; // FIX
    private CkanClient ckanClient;

    public ChangesManager(CkanClient ckanClient) {
        this.ckanClient = ckanClient;
    }

    /**
     * @param lastAppliedChangesId changesId of the last changes batch that has been applied before the new initial batch
     *                             was created.
     *                             downloads changes batches (with changesId larger than provided argument) from dataset "register-adries-ra-zmenove-davky" and saves them in
     *                             a directory specified by CHANGES_DIRECTORY_PATH constant.
     */
    public void downloadChanges(long lastAppliedChangesId, String changesDirPath) throws Exception {
        JsonObject datasetDetail = ckanClient.callPackageShow(CHANGES_PACKAGE_ID);

        JsonArray changesResources = datasetDetail.getJsonObject("result").getJsonArray("resources");
        for (int i = 0; i < changesResources.size(); i++) {
//            System.out.println(i + " \\ " + changesResources.size());
            JsonObject resource = changesResources.getJsonObject(i);
            JsonString initBatchJson = resource.getJsonString("initial_batch");
            if (initBatchJson == null) {
                JsonString changesIdJson = resource.getJsonString("changes_id");
                if (changesIdJson != null) {
                    long changesId = Long.parseLong(changesIdJson.getString());
                    if (changesId > lastAppliedChangesId) {
//                        JsonString fileNameJson = resource.getJsonString("file_name");
//                        if (fileNameJson == null) {
//                            System.err.println("unable to parse changes batch's file name, going on to next one.");
//                            continue;
//                        }
//                        String fileName = fileNameJson.getString().replace("_","");

                        String fileName = "zmenovadavka" + changesId + ".xml";
                        System.out.println("Going to download " + fileName);
                        String resourceId = resource.getString("id");
                        JsonObject response = ckanClient.callResourceShow(resourceId);
                        String url = response.getJsonObject("result").getString("url");

                        String xmlChangesBatch = downloadResource(url);

                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(changesDirPath + fileName))) {
                            writer.write(xmlChangesBatch);
                        }
                        System.out.println("File " + fileName + " has been successfully saved");
                    }
                }
            }
        }
    }
    public void downloadChanges2018(String changesDirPath) throws Exception {
//        String url = "http://127.0.0.1/dataset/7c196993-fcd2-407c-ac6c-f4a4ca68cef5/resource/12637ee7-9bd4-4e7d-a680-abefa27e8d6c/download/zmenovedavky0612.2018.zip";
//        String url = "http://127.0.0.1/dataset/7c196993-fcd2-407c-ac6c-f4a4ca68cef5/resource/12637ee7-9bd4-4e7d-a680-abefa27e8d6c/download/zmenovedavky0612.2018.zip";
        String url = "http://127.0.0.1/dataset/7c196993-fcd2-407c-ac6c-f4a4ca68cef5/resource/12637ee7-9bd4-4e7d-a680-abefa27e8d6c/download/zmenovedavkyra.0612.2018.zip";
//        String url = "https://data.gov.sk/dataset/7c196993-fcd2-407c-ac6c-f4a4ca68cef5/resource/12637ee7-9bd4-4e7d-a680-abefa27e8d6c/download/zmenovedavkyra.0612.2018.zip";
        File zip = new File(changesDirPath + "changes2018.zip");
        FileUtils.copyURLToFile(new URL(url), zip);

        ZipInputStream zis = new ZipInputStream(new FileInputStream(zip));
        ZipEntry zipEntry = zis.getNextEntry();
        byte[] buffer = new byte[1024];
        while (zipEntry != null) {
            File newFile = new File(changesDirPath + zipEntry.getName());
            FileOutputStream fos = new FileOutputStream(newFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
        FileUtils.deleteQuietly(zip);
    }


    /**
     * uploads the changes batches to the appropriate datastore table with either insert or upsert method, depending on
     * whether it is the "changed" or "consolidated" datastore. then updates metadata of the particular resources in the
     * same way the original RA process does it when applying a new changes batch.
     */

    public void uploadChangesThirdDS(Registers.Register register, String changesDirPath) throws Exception {
        XmlToJsonTransformer xmlToJsonTransformer = new XmlToJsonTransformer();
        List<File> xmlResources = loadXmlResources(changesDirPath);

        for (File xmlResource : xmlResources) {
            Map<String, String> paramsToUpdate = new HashMap<>();
            String changesId = parseChangesId(xmlResource.getName());
            System.out.println("Applying Zmenova davka c. " + changesId + " to register " + register.getResourceNameBase());
            JsonArray jsonRecordsInsert = xmlToJsonTransformer.transformXmlToJson(xmlResource, register, "CHANGES");

            String response = ckanClient.updateDatastore(register.getThirdId(), jsonRecordsInsert, "insert");
            // updating changed resource's metadata (last_changes_id)
            paramsToUpdate.put("last_changes_id", changesId);
            ckanClient.updateResource(register.getThirdId(), paramsToUpdate);
        }

    }
    public void uploadChanges(Registers.Register register, String changesDirPath) throws Exception {
        XmlToJsonTransformer xmlToJsonTransformer = new XmlToJsonTransformer();
        List<File> xmlResources = loadXmlResources(changesDirPath);

        for (File xmlResource : xmlResources) {
            Map<String, String> paramsToUpdate = new HashMap<>();
            String changesId = parseChangesId(xmlResource.getName());
            System.out.println("Applying Zmenova davka c. " + changesId + " to register " + register.getResourceNameBase());
            JsonArray jsonRecordsInsert = xmlToJsonTransformer.transformXmlToJson(xmlResource, register, "CHANGES");

            String response = ckanClient.updateDatastore(register.getChangedId(), jsonRecordsInsert, "insert");
            // new: uploading to an extra init+changes resource
            String response2 = ckanClient.updateDatastore(register.getThirdId(), jsonRecordsInsert, "insert");
            // updating changed resource's metadata (last_changes_id)
            paramsToUpdate.put("last_changes_id", changesId);
            ckanClient.updateResource(register.getChangedId(), paramsToUpdate);
            ckanClient.updateResource(register.getThirdId(), paramsToUpdate);

            JsonArray jsonRecordsUpsert = xmlToJsonTransformer.transformXmlToJson(xmlResource, register, "CONSOLIDATED_UPSERT");
            String response3 = ckanClient.updateDatastore(register.getConsolidatedId(), jsonRecordsUpsert, "upsert");
            // updating consolidated resource's metadata (last_changes_id and last_update_successful)
            paramsToUpdate.put("last_update_successful", changesId);
            ckanClient.updateResource(register.getConsolidatedId(), paramsToUpdate);
        }
    }


    private String downloadResource(String url) throws Exception {
//        String url = CHANGES_BATCHES_RESOURCE_ID +
//                id + "/download/" + fileName;

        HttpResponse response = ckanClient.executeHttpGetRequest(url);

        BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }

    public static List<File> loadXmlResources(String path) {
        File sourceDirectory = new File(path);

        File[] changesFiles = sourceDirectory.listFiles();
        if (changesFiles == null || changesFiles.length < 1) {
            System.out.println("Changes folder is empty, so no new changes batches are going to be applied.");
            return Collections.emptyList();
        }
        ArrayList<File> resources =  new ArrayList<>(Arrays.asList(changesFiles));
        resources.sort((o1, o2) -> {
            int changesId1 = Integer.parseInt(parseChangesId((o1).getName()));
            int changesId2 = Integer.parseInt(parseChangesId((o2).getName()));
            return  changesId1 - changesId2;
        });
        return resources;
    }

    public static String parseChangesId(String fileName) {
        Pattern pattern = Pattern.compile("[a-z]+(\\d+)(.xml)");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.matches()){
            return matcher.group(1);
        }
        throw new RuntimeException("Unable to parse changes_id from file name " + fileName + ". Make sure" +
                "it has the proper format zmenovadavkaX.xml where X stands for changes_id of the particular" +
                "changes file.");
    }
}
