package ra;

import org.apache.commons.io.FileUtils;
import ra.additional_fixes.Main;
import ra.ckan.ResourceInitializer;

//import javax.annotation.Resource;
import javax.json.JsonObject;
import java.io.*;

/**
 * Class that creates testing registers with resources as in DATA.GOV.SK
 * Used only for testing purposes
 */
public class TestDataCreator {
    private static final String TEST_SOURCE_DIR_PATH = "src/main/resources/test/";

    private Registers registers;
    private CkanClient ckanClient;
    private String testSuffix; //since we are not able to call dataset_purge
    private String orgId;

    public TestDataCreator(Registers registers, CkanClient ckanClient, String orgId, String testSuffix) {
        this.registers = registers;
        this.orgId = orgId;
        this.testSuffix = testSuffix;
        this.ckanClient = ckanClient;
    }

    public void createTestingRegisters() throws Exception {
        ResourceInitializer resourceInit = new ResourceInitializer(ckanClient);
        initializeDatasets(testSuffix);

        for(Registers.Register register : registers.getAllRegisters()){
            resourceInit.createEmptyCsvResources(register, testSuffix);
            resourceInit.initializeDatastoresFromInitBatch(register, ra.additional_fixes.Main.INIT_JSON_DIRECTORY_PATH);
            File testResourcesDirectory = new File(TEST_SOURCE_DIR_PATH + register.getResourceNameBase());
            File[] files = testResourcesDirectory.listFiles();
            if(files == null || files.length < 1){
                throw new RuntimeException("Nothing to upload, make sure the test resource directories are not empty.");
            }
            for(File file : files) {
                String periodicity = guessPeriodicityFromFileName(file.getName());
                String format = guessFormatFromFileName(file.getName());
                String description = guessDescriptionFromFileName(file.getName());
                System.out.println("going to upload file " + file.getName());
                ckanClient.createResource(file.getName(), register.getName() + testSuffix,
                        periodicity, format, description,file);
            }
        }

        Utils.storeResourceIdsToFile(registers, Main.RESOURCE_IDS_FILE_PATH);

    }


    private void initializeDatasets(String testSuffix) throws IOException {
        for(Registers.Register register : registers.getAllRegisters()){
            JsonObject response = ckanClient.createPackage(register.getName() + testSuffix, register.getTitle(), orgId);
        }
    }


    public void runDelete(String zipDirPath) throws IOException {
        for(Registers.Register register : registers.getAllRegisters()){
            ckanClient.deletePackage(register.getName() + testSuffix);
            File zipDirectory = new File(zipDirPath + register.getResourceNameBase());
            FileUtils.deleteDirectory(zipDirectory);
        }
    }

    private static String guessFormatFromFileName(String fileName){
        if(fileName.endsWith("dáta")) {
            return "api, csv";
        }
        else if (fileName.startsWith("Dokumentácia")){
            return "HTML";
        }
        return "XML";
    }

    private static String guessPeriodicityFromFileName(String fileName) {
        if(fileName.endsWith("dáta")) {
            return "daily";
        }
        else if (fileName.startsWith("Dokumentácia")){
            return "irregularly";
        }
        return "semi-annually";
    }
    private String guessDescriptionFromFileName(String fileName) {
        if (fileName.startsWith("Dokumentácia")){
            return Registers.getDocResourceDescription();
        }else if (fileName.contains("Init")){
            return Registers.getInitBatchResourceDescription();
        }
        return "";
    }
}
