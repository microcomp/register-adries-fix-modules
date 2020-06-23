package ra.ckan;

import org.apache.commons.codec.Charsets;
import ra.CkanClient;
import ra.Registers;

import javax.json.*;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;


public class ResourceInitializer {
    private CkanClient ckanClient;

    public ResourceInitializer(CkanClient ckanClient) {
        this.ckanClient = ckanClient;
    }

    /**
     * Creates empty resources ready for the datastore to be initialized in.
     * Used only for testing purposes.
     */
    public void createEmptyCsvResources(Registers.Register register, String testSuffix) throws IOException {
        String changedName = register.getResourceNameBase() + " - zmenové dáta";
        String consolidatedName = register.getResourceNameBase() + " - konsolidované dáta";
        String packageId = register.getName() + testSuffix;
//            Map<String, String> urlUpdate = new HashMap<>();

        JsonObject response = ckanClient.createResource(changedName, packageId, "daily","csv");
        String changedId = response.getJsonObject("result").getString("id");

        register.setChangedId(changedId);

        response = ckanClient.createResource(consolidatedName, packageId, "daily","csv");
        String consolidatedId = response.getJsonObject("result").getString("id");

        register.setConsolidatedId(consolidatedId);
    }

    public void createExtraDatastore(Registers.Register register) throws IOException {
        String name = register.getResourceNameBase() + " - inicializačné a zmenové dáta";

        String packageId = register.getName();
        String description = Registers.getThirdDSResourceDescription();

        JsonObject response = ckanClient.createDatastoreResource(name, packageId, description, false);
        String thirdId = response.getJsonObject("result").getString("id");
        register.setThirdId(thirdId);
    }



    /**
     * Creates empty datastore with all table columns corresponding to the given register, that are
     * set in the Registers.Register class.
     * Initializes the 'consolidated' datastore from initial batches previously transformed to .json format. Leaves the
     * 'changed' datastore empty and ready to accept new changes coming from RA_changes webservice.
     */
    public void initializeDatastoresFromInitBatch(Registers.Register register, String initBatchDirectoryPath) throws Exception {
        JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.emptyMap());

        ckanClient.createDatastore(register.getChangedId(), register.getChangedTableFields(), new String[] {} );
        ckanClient.createDatastore(register.getThirdId(), register.getChangedTableFields(), new String[] {} );

        ckanClient.createDatastore(register.getConsolidatedId(), register.getConsolidatedTableFields(), register.getPrimaryKeys());

        File dir = new File(initBatchDirectoryPath + register.getRegisterType().name());
        if (!dir.exists()){
            throw new RuntimeException("Couldn't locate the directory with JSON initial batch for register " +
                    register.getRegisterType().name() + ". Make sure the transformation process has been executed properly.");
        }
        File[] chunks = dir.listFiles();
        if (chunks == null || chunks.length < 1) {
            throw new RuntimeException("The directory that is supposed to contain the JSON initial batch for register " +
                    register.getRegisterType().name() + " is empty. Make sure the transformation process has been executed properly.");
        }
        Arrays.sort(chunks, Comparator.comparingInt(file -> Integer.parseInt(file.getName().replaceAll(".json", ""))));
        int len = chunks.length;
        int i = 0;
        for(File chunk : chunks) {
            System.out.println("Inserting chunk " + ++i + "/" + len );
            JsonReader jsonReader = readerFactory.createReader(new FileInputStream(chunk), Charsets.UTF_8);
            JsonArray jsonRecords = jsonReader.readArray();
            System.out.println("Inserting to consolidated datastore...");
            ckanClient.updateDatastore(register.getConsolidatedId(), jsonRecords, "insert");
            System.out.println("Inserting to init+changed datastore...");
            ckanClient.updateDatastore(register.getThirdId(), jsonRecords, "insert");
        }
    }
}
