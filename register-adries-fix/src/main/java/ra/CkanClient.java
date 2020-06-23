package ra;

import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

import javax.json.*;
import javax.json.stream.JsonParsingException;
import javax.net.ssl.SSLContext;
import java.io.*;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles entire communication with CKAN API through HTTP requests.
 */
public class CkanClient{
    /* production database */
    private static final String URL = "http://127.0.0.1";
//    private static final String URL = "https://data.gov.sk/";
    /* testing databases */
//    private static final String TEST_URL = "http://192.168.99.41:5000";
    private static final String TEST_URL = "https://10.31.123.100";
//    private static final String TEST_URL = "http://localhost";

    /* maximum size of json data to be sent in one request */
    private static final int CHUNK_MAX_SIZE = 20000;

    private final RequestBuilder requestBuilder;
    private final String url;
    private final String apiUrl;
    private final String apiKey;


    /**
     * @param useTestDatabase true if we want to use the testing database, false otherwise
     */
    public CkanClient(String apiKey, boolean useTestDatabase){
        requestBuilder = new RequestBuilder();
        this.apiKey = apiKey;
        url = useTestDatabase ? TEST_URL : URL;
        apiUrl = url + "/api/action/";
    }

    /**
     * used only for testing purposes
     */
    public JsonObject createPackage(String name, String title, String ownerOrg) throws IOException {
        //won't be used
        Map<String, String> params = new HashMap<>();
        params.put("name", name); //nesmie sa opakovat
        params.put("title", title);
        params.put("private", "false");
        params.put("owner_org", ownerOrg);
        params.put("spatial","{ \"type\": \"Polygon\",  \"coordinates\": [[[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]]}");

        String req = requestBuilder.buildJsonHttpRequest(params);
        HttpEntity entity = new StringEntity(req, Charsets.UTF_8);

        return executeHttpPostRequest(entity, "package_create");
    }

    /**
     * updates the given parameters of a ckan resource
     */
    public JsonObject updateResource(String resourceId, Map<String, String> paramsToUpdate) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("id", resourceId);
        System.out.println("ID to update " + resourceId);
        params.putAll(paramsToUpdate);

        String request = requestBuilder.buildJsonHttpRequest(params);
        HttpEntity entity = new StringEntity(request, Charsets.UTF_8);

        return executeHttpPostRequest(entity, "resource_update");
    }

    /**
     *  used for updating docs only
     */
    public JsonObject updateResourceFile(String resourceId, File file) throws IOException {

        System.out.println("ID to update " + resourceId);
//        params.putAll(paramsToUpdate);
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addTextBody("id", resourceId, ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        builder.addBinaryBody("upload", file, ContentType.DEFAULT_BINARY, file.getName());

//        String request = requestBuilder.buildJsonHttpRequest(params);
        HttpEntity entity = builder.build();

        return executeHttpPostRequest(entity, "resource_update");
    }



    public JsonObject createResource(String name, String packageId, String periodicity, String format) throws IOException {
        return createResource(name, packageId, periodicity, format, null, null);
    }

    /**
     * creates a ckan resource using a multipart/form http request (either empty or from a file)
     */
    public JsonObject createResource(String name, String packageId, String periodicity, String format, String description, File file) throws IOException {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addTextBody("name", name, ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("package_id", packageId,  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("url", " ",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("status", "public",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("validity", "perm_valid",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("data_correctness", "correct and exact",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("periodicity", periodicity,  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("format", format,  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));

        if(description != null) {
            builder.addTextBody("description", description, ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        }

        if(file != null) {
            builder.addBinaryBody("upload", file, ContentType.DEFAULT_BINARY, file.getName());
        } else {  /* pri file uploade sa automaticky doplni url, v opacnom pripade ide o testovacie volanie a doplnime neplatnu
                    datastore url tak ako je v DATA.GOV.SK */
            builder.addTextBody("url_type", "datastore",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        }
        HttpEntity entity = builder.build();
        return executeHttpPostRequest(entity, "resource_create");
    }

    // new

        public JsonObject createDatastoreResource(String name, String packageId, String description, boolean consolidated) throws IOException {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addTextBody("name", name, ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("package_id", packageId,  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("url", " ",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("status", "public",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("validity", "custom_valid",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("validity_description", "validFrom, validTo, effectiveDate",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("data_correctness", "correct and exact",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("periodicity", "daily",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8))
                .addTextBody("format", "api, csv",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
//                .addTextBody("schema", "",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));

        builder.addTextBody("description", description, ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        builder.addTextBody("maintainer", "E8943CC3-CDF2-4368-A29F-FFAC8E09F443", ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        builder.addTextBody("url_type", "datastore",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        builder.addTextBody("last_changes_id", "0",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));
        if (consolidated)
            builder.addTextBody("last_update_successful", "0",  ContentType.APPLICATION_JSON.withCharset(Charsets.UTF_8));

        HttpEntity entity = builder.build();
        return executeHttpPostRequest(entity, "resource_create");
    }

    /**
     * creates an empty datastore in the given (already existing) resource
     * @param primaryKeys - list of unique keys (existing column names), used by the upsert method to replace records
     *                    in datastore table
     * @param fields - table column names mapped to their valid datatypes
     */
    public String createDatastore(String resourceId, Map<String, String> fields, String[] primaryKeys) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("resource_id", resourceId);
        params.put("force", "true");

        if(primaryKeys != null && primaryKeys.length > 0){
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < primaryKeys.length; i++){
                builder.append(primaryKeys[i]);
                if(i < primaryKeys.length - 1) builder.append(", ");
            }
            params.put("primary_key", builder.toString());
        }

        String jsonRequest = requestBuilder.buildDatastoreCreateWithFields(params, fields);
        HttpEntity entity = new StringEntity(jsonRequest, Charsets.UTF_8);

        JsonObject jsonResponse = executeHttpPostRequest(entity, "datastore_create");

        return jsonResponse.toString();
    }

    /**
     * calls datastore_upsert method with the specified method (insert/upsert) and given jsonRecords.
     * splits the records into chunks and sends them separately in case they are too large to be sent in one API request
     */
    public String updateDatastore(String resourceId, JsonArray jsonRecords, String method) throws Exception {
        if (jsonRecords != null && jsonRecords.size() != 0) {
            System.out.println("Going to update datastore for resource " + resourceId + " using method " + method);
            Map<String, String> metadata = new HashMap<>();
            metadata.put("resource_id", resourceId);
            metadata.put("method", method);
            metadata.put("force", "true");
            JsonObject responseJson;
            String response = "";
            HttpEntity entity;

            if (jsonRecords.size() > CHUNK_MAX_SIZE) {
                System.out.println("JSON array is too large to be sent in one request, going to split ...");
                JsonValue json;
                int counter = 0;
                int chunkCounter = 1;
                String data;
                JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
                JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
                for (JsonValue jsonRecord : jsonRecords) {
                    json = jsonRecord;
                    arrayBuilder.add(json);
                    ++counter;
                    if (counter == CHUNK_MAX_SIZE) {
                        data = requestBuilder.buildDatastoreRequest(metadata, arrayBuilder.build());
                        entity = new StringEntity(data, Charsets.UTF_8);
                        executeHttpPostRequest(entity, "datastore_upsert");
                        arrayBuilder = factory.createArrayBuilder();
                        counter = 0;
                        ++chunkCounter;
                    }
                }

                data = requestBuilder.buildDatastoreRequest(metadata, arrayBuilder.build());
                entity = new StringEntity(data,Charsets.UTF_8);
                responseJson = executeHttpPostRequest(entity, "datastore_upsert");
                System.out.println("Datastore update for resource " + resourceId + " successfully ended");
            }

            else {
                System.out.println("No need to split JSON array, sending it to CKAN in one request ...");
                String data = requestBuilder.buildDatastoreRequest(metadata, jsonRecords);
                entity = new StringEntity(data, Charsets.UTF_8);
                responseJson = executeHttpPostRequest(entity, "datastore_upsert");
                System.out.println("Update datastore for resource " + resourceId + " successfully ended");
            }

            if (responseJson != null) {
                response = responseJson.toString();
            }

            return response;
        } else {
            System.out.println("Empty Json array received, nothing to do...");
            return null;
        }
    }

    public JsonObject searchDatastore(String resourceId, Map<String, String> filters) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("resource_id", resourceId);

        String request = requestBuilder.buildDatastoreSearch(params, filters);

        HttpEntity entity = new StringEntity(request, Charsets.UTF_8);
        return executeHttpPostRequest(entity, "datastore_search");
    }

    /**
     * removes the datastore table from the given resource, leaving the resource empty.
     */
    public JsonObject deleteDatastore(String resourceId) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("resource_id", resourceId);
        params.put("force", "true");

        HttpEntity entity = new StringEntity(requestBuilder.buildJsonHttpRequest(params), Charsets.UTF_8);

        return executeHttpPostRequest(entity, "datastore_delete");
    }

    /**
     * deletes an entire resource from a dataset
     */
    public JsonObject deleteResource(String resourceId) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("id", resourceId);
        String request = requestBuilder.buildJsonHttpRequest(params);
        HttpEntity entity = new StringEntity(request, Charsets.UTF_8);
        return executeHttpPostRequest(entity, "resource_delete");
    }

    /**
     * deletes an entire dataset
     * only used for testing purposes
     */
    public JsonObject deletePackage(String packageId) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("id", packageId);
        String request = requestBuilder.buildJsonHttpRequest(params);
        HttpEntity entity = new StringEntity(request, Charsets.UTF_8);
        return executeHttpPostRequest(entity, "package_delete");
    }

    public JsonObject callPackageShow(String packageId) throws IOException {
        String url = apiUrl + "package_show?id=" + packageId;
        System.out.println(url);
        HttpResponse response = executeHttpGetRequest(url);
        return handleJsonResponse(response);
    }

    public JsonObject callResourceShow(String resourceId) throws IOException {
        String url = apiUrl + "resource_show?id=" + resourceId;
        HttpResponse response = executeHttpGetRequest(url);
        return handleJsonResponse(response);
    }


    /**
     * @param entity http entity containing json parameters valid for given API action
     * @param action ckan API action to be executed
     */
    public JsonObject executeHttpPostRequest(HttpEntity entity, String action) throws IOException {
        String url = apiUrl + action;
//        HttpClient client = HttpClients.createDefault();

        CloseableHttpClient client = null;
        // BYPASSES SSL CERTIFICATE VERIFICATION
        try {
            SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial((x509Certificates, s) -> true).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslcontext,
                    SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            client = HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            throw new RuntimeException("Error while creating certificate-unaware http client.");
        }

        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Authorization", apiKey);

        /* otherwise the ckan API wouldn't recognize the data as json*/
        if(entity instanceof StringEntity){
            httpPost.addHeader("content-type", "application/json");
        }

        httpPost.setEntity(entity);
        HttpResponse response = client.execute(httpPost);
        JsonObject ckanResponse;
        if (!isHttpSuccess(response)){
            Utils.ckanResponseToFile(response);
            throw new RuntimeException("HTTP POST request wasn't successful. The ckan response has been saved to the error.json file.");
        }
        ckanResponse = handleJsonResponse(response);
//        System.out.println("CKAN action " + action + " was executed successfully.");

        return ckanResponse;
    }

    public HttpResponse executeHttpGetRequest(String url) throws IOException {
//        CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpClient client = null;
        try {
            // BYPASSES SSL CERTIFICATE VERIFICATION
            SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial((x509Certificates, s) -> true).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslcontext, //for you this is builder.build()
                    SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            client = HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            throw new RuntimeException("Error while creating certificate-unaware http client.");
        }
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);
        if (!isHttpSuccess(response)){
            Utils.ckanResponseToFile(response);
            throw new RuntimeException("HTTP GET request wasn't successful.The ckan response has been saved to the error.json file.");
        }
        return response;
    }


    /**
     * throws exception if the executed ckan request failed. saves the ckan error response to the specified file
     * for further debugging.
     */
    private JsonObject handleJsonResponse(HttpResponse response) throws IOException {
        JsonReaderFactory readerFactory = Json.createReaderFactory(Collections.<String, Object>emptyMap());
        JsonReader reader = readerFactory.createReader(response.getEntity().getContent(), Charsets.UTF_8);
        JsonObject ckanResponse;
        try {
            ckanResponse = reader.readObject();
            if (!ckanResponse.getBoolean("success")) {
                Utils.ckanResponseToFile(response);
                throw new RuntimeException("CKAN action failed with error: " + ckanResponse.getJsonObject("error"));
            }
        } catch(JsonParsingException e) {
            Utils.ckanResponseToFile(response);
            throw new RuntimeException("exception while parsing CKAN response. Ckan response have been saved to the error file");
        }

        return ckanResponse;
    }

    /**
     * @return true if the http request ended with status code 200, false otherwise.
     * also prints out the error message to error output
     */
    private boolean isHttpSuccess(HttpResponse response){
        if (response.getStatusLine().getStatusCode() != 200) {
            StringBuilder responseAsString = new StringBuilder();
            responseAsString.append(response.getStatusLine().toString()).append('\n');
            Header[] headers = response.getAllHeaders();

            for (Header h : headers) {
                responseAsString.append(h.toString()).append('\n');
            }
            System.err.println("CKAN request was probably not successful. Received HTTP status and headers:\n" + responseAsString);
            return false;
        }
        return true;
    }

    public String getUrl(){
        return url;
    }
}
