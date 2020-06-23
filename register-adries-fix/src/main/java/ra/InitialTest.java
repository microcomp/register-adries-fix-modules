package ra;

import org.apache.commons.io.FileUtils;
import ra.additional_fixes.Main;

import javax.json.JsonObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class InitialTest {
    private CkanClient ckanClient;

    public InitialTest(CkanClient ckanClient) {
        this.ckanClient = ckanClient;
    }

    public void runInitialTest(String resourceDirectory, String testSuffix) throws IOException {
        System.out.println("GOING TO RUN AN INITIAL TEST TO MAKE SURE CKAN API CALLS WORK PROPERLY");
        String testDatasetName = "register-adries-register-ulic" + testSuffix;

        InputStream is = Main.class.getResourceAsStream("/testfile.xml");
        File testFile = new File(resourceDirectory + "testfile.xml");
        FileUtils.copyInputStreamToFile(is, testFile);
        is.close();

        System.out.println("trying to upload testing file to dataset " + testDatasetName);
        JsonObject response = ckanClient.createResource("upload test", testDatasetName, "irregularly", "xml", "test", testFile);
        System.out.println("file successfully uploaded");

        String resourceId = response.getJsonObject("result").getString("id");
        System.out.println("trying to delete testing file from dataset " + testDatasetName);
        ckanClient.deleteResource(resourceId);
        System.out.println("testing file successfully deleted");
        System.out.println("\nCKAN API calls should work properly. Test passed.\n");

        FileUtils.deleteQuietly(testFile);
    }
}
