package ra.ckan;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import ra.CkanClient;
import ra.Registers;
import ra.Utils;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * handles downloading, deleting, zipping and uploading of the original resources as well as uploading the newly
 * created resources and updating datastores.
 */
public class ResourceManager {
    private static final long MAX_XML_UPLOAD_SIZE = 20000; // (20MB)

    private CkanClient ckanClient;
    private String timestamp;

    public ResourceManager(CkanClient ckanClient) {
        this.ckanClient = ckanClient;
        timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
    }


    /**
     * completely removes the original initial batch and documentation resources from datasets, or deletes the datastore
     * tables from datastore resources, so that the resource ids and metadata stay the same.
     */
    public void clearDatasets(Registers.Register register, String testSuffix) throws IOException {
        JsonObject packageShow = ckanClient.callPackageShow(register.getName() + testSuffix);

        JsonArray resources = packageShow.getJsonObject("result").getJsonArray("resources");
//            Utils.jsonStringToFile(resources.toString());
        for(JsonObject resource : resources.getValuesAs(JsonObject.class)) {
            String resourceId = resource.getString("id");

            boolean datastoreActive = resource.getBoolean("datastore_active");
            if (datastoreActive){
                //if (resourceId.equals(register.getChangedId()) || resourceId.equals(register.getConsolidatedId())){
                System.out.println("Going to empty the datastore in resource " + resourceId);
                ckanClient.deleteDatastore(resourceId);
            }
            else {
                System.out.println("Going to delete " + resourceId + " entirely.");
                ckanClient.deleteResource(resourceId);
            }
        }
    }


    /**
     * Downloads the original resources from dataset and creates an archive separately for each dataset.
     * The download urls of datastore resources have the following form in DATA.GOV.SK : "/datastore/dump/<resource_id>",
     * so we have to prepend the ckan domain name.).
     * Be careful with altering the path where the zipped files are being saved.
     */
    public void downloadAndZip(Registers.Register register, String zipDirPath, String testSuffix) throws Exception {
        System.out.println("Going to download resources from " + register.getResourceNameBase() + " dataset.");
        //if you alter this, you must adjust the getPathToZipFile() method too.
        File directory = new File(zipDirPath + register.getResourceNameBase());
        if(!directory.exists()){
            directory.mkdir();
        }

        JsonObject packageShow = ckanClient.callPackageShow(register.getName() + testSuffix);
        JsonArray resources = packageShow.getJsonObject("result").getJsonArray("resources");
        for(JsonObject resource : resources.getValuesAs(JsonObject.class)) {
            JsonString urlJson = resource.getJsonString("url");
            JsonString nameJson = resource.getJsonString("name");
            if(urlJson == null || nameJson == null) {
                System.err.println("important metadata are missing, unable to download resource.");
                continue;
            }
            String downloadUrl = resource.getString("url");
            String fileName = resource.getString("name");
            if(isDatastoreUrl(downloadUrl)){
                downloadUrl = ckanClient.getUrl() + "/datastore/dump/" +  resource.getString("id");
            }
            
            //FIX to override ssl handshake
            downloadUrl= downloadUrl.replace("https://10.31.123.100", ckanClient.getUrl());
            downloadUrl= downloadUrl.replace("https://data.edovfix.gov.sk", ckanClient.getUrl());
            downloadUrl= downloadUrl.replace("https://data.gov.sk", ckanClient.getUrl());
            
            System.out.println("Downloading from URL: " +downloadUrl);
            //if you alter this, you must adjust the getPathToZipFile() method too.
            FileUtils.copyURLToFile(new URL(downloadUrl), new File(directory + "/" + fileName));
        }
        System.out.println("Download complete, going to zip all of the files in directory.\n");

        String archiveName = getZipFileName(register);
        zipAllFilesInDirectory(archiveName, directory);
    }
    
    /**
     * The name of the archive contains a timestamp of the date on which it was created. This method is not going to be
     * able to retrieve the zipped archives if the downlaodAndZip() and uploadZipFiles() methods were called on different
     * days.
     * NOT IN USE.
     */

    public void uploadZipFiles(Registers.Register register, String zipDirPath, String testSuffix) throws IOException {
        // USES TODAY'S DATE TO LOOK FOR THE ZIP FILE.
        // NOT GOING TO WORK IF THE ZIP FILE WAS CREATED ON A DIFFERENT DAY.
        File archive = new File(getPathToZipFile(register, zipDirPath) + getZipFileName(register));
        if(!archive.exists()) return;
        ckanClient.createResource(archive.getName(), register.getName() + testSuffix,
                "irregularly","ZIP", Registers.getArchiveResourceDescription(), archive);
    }


    /**
     * Uploads the upgraded documentations located in docDirPath, points the datastore
     * resources to new docs by updating their schema metadata, then zips (if necessary) and uploads the new initial batch located
     * in inixXmlPath.
     */
    public void uploadDocsAndInitBatches(Registers.Register register, String docDirPath, String initXmlPath, String testSuffix) throws IOException {
        File doc = new File(docDirPath + register.getDocFileName());

        JsonObject response = ckanClient.createResource("Dokumentácia datasetu", register.getName() + testSuffix,
                "irregularly","HTML", Registers.getDocResourceDescription(), doc);
        JsonString url = response.getJsonObject("result").getJsonString("url");

        if (url != null) {
            try {

            Map<String, String> paramsToUpdate = new HashMap<>();
            paramsToUpdate.put("schema", url.getString());
            ckanClient.updateResource(register.getChangedId(), paramsToUpdate);
            ckanClient.updateResource(register.getConsolidatedId(), paramsToUpdate);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        File init = new File(initXmlPath + register.getXmlFileName());
        long fileSize = FileUtils.sizeOf(init) / 1000;
        if (fileSize > MAX_XML_UPLOAD_SIZE) {
            System.out.println("going to compress file " + init.getName());
            File zippedFile = zipFile(init.getName().replace(".xml", ".zip"), init);
            ckanClient.createResource("Inicializačná dávka", register.getName() + testSuffix,
                    "semi-annually", "ZIP", Registers.getInitBatchResourceDescription(), zippedFile);
        }
        else {
            ckanClient.createResource("Inicializačná dávka", register.getName() + testSuffix,
                    "semi-annually", "XML", Registers.getInitBatchResourceDescription(), init);
        }
    }



    private File zipFile(String archiveName, File file) throws IOException {
        byte[] buffer = new byte[1024];
        if (!file.exists() || file.isDirectory()) {
            throw new RuntimeException("File to be zipped does not exist or is not a file");
        }
        File zipFile = new File(file.getPath().replace(file.getName(), archiveName));
        OutputStream output = new FileOutputStream(zipFile);
        ZipOutputStream zipOutput = new ZipOutputStream(output, Charsets.UTF_8);
        try {
            InputStream fileInput = new FileInputStream(file);
            ZipEntry zipEntry = new ZipEntry(Utils.removeDiacritics(file.getName()));
            zipOutput.putNextEntry(zipEntry);
            int len;
            while((len = fileInput.read(buffer)) > 0) {
                zipOutput.write(buffer, 0, len);
            }
            zipOutput.closeEntry();
            try {
                fileInput.close();
            } catch(IOException e) {
                System.err.println("Failed to close XML file stream due to exception " + e.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to zip files due to exception " +  e.toString());
        } finally {
            try {
                zipOutput.close();
            } catch (IOException e) {
                System.err.println("Failed to close ZIP archive due to exception " + e.toString());
            }
        }
        return zipFile;
    }

    private void zipAllFilesInDirectory(String archiveName, File directory) throws IOException  {
        byte[] buffer = new byte[1024];
        if (!directory.exists() || !directory.isDirectory()) {
            throw new RuntimeException("Directory to be zipped does not exist or is not a directory");
        }

        File[] files = directory.listFiles();
        if (files == null || files.length < 1) {
            return;
        }
        //if you alter this, you must adjust the getZipFilePath() method too.
        File zipFile = new File(directory.getPath() + "/" + archiveName);
        OutputStream output = new FileOutputStream(zipFile);
        ZipOutputStream zipOutput = new ZipOutputStream(output, Charsets.UTF_8);
        InputStream fileInput;
        ZipEntry zipEntry;

        try {
            for (File file : files) {
                zipEntry = new ZipEntry(Utils.removeDiacritics(file.getName()));
                zipOutput.putNextEntry(zipEntry);
                fileInput = new FileInputStream(file);

                int len;
                while ((len = fileInput.read(buffer)) > 0) {
                    zipOutput.write(buffer, 0, len);
                }
                zipOutput.closeEntry();
                try {
                    fileInput.close();
                } catch (IOException e) {
                    System.err.println("Failed to close file due to exception " + e.toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to zip files due to exception " +  e.toString());
        } finally {
            try {
                zipOutput.close();
            } catch (IOException e) {
                System.err.println("Failed to close ZIP archive due to exception " + e.toString());
            }
        }
    }

    private String getPathToZipFile(Registers.Register register, String zipDirPath){
        return zipDirPath + register.getResourceNameBase() + "/";
    }
    private String getZipFileName(Registers.Register register){
        return register.getResourceNameBase() + "_archiv_" + timestamp + ".zip";
    }


    public static boolean isDatastoreUrl(String url) {
        return url.contains("/datastore/dump/");
    }
}
