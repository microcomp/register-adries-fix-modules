package ra.transform;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import ra.Main;
import ra.Registers;

import javax.json.*;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Copied from ra_ws_muleproccess project.
 * Minor optimizations have been made.
 */

public class XmlToJsonTransformer {
//    private static final String CONFIGURATION_CSV_FILE_PATH = "src/main/resources/xml_to_json_transform_config.csv";
    private static final int CONFIGURATION_COLUMN_COUNT = 7;
    private static final int MAX_CHUNK_SIZE = 40000; //up to 20 MB
    private static final String NAMESPACE_MAPPING = "minv";
    private static final String SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private ArrayList<TransformRule> allTransformRules = new ArrayList<>();

    public XmlToJsonTransformer() throws Exception {
        this.initFromCsv();
    }

    private void initFromCsv() throws FileNotFoundException {
        InputStream is = getClass().getResourceAsStream("/xml_to_json_transform_config.csv");
//        is = new FileInputStream(CONFIGURATION_CSV_FILE_PATH);
        Throwable var3 = null;

        try {
            Reader reader = new InputStreamReader(is);
            Throwable var5 = null;

            try {
                ICsvListReader listReader = new CsvListReader(reader, CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
                Throwable var7 = null;

                try {
                    listReader.getHeader(true);

                    List record;
                    while((record = listReader.read()) != null) {
                        if (record.size() != 7) {
                            throw new IllegalStateException("Nekorektna definicia riadku konfiguracie. Pocet buniek nie je 7. Obsah riadku: " + listReader.getUntokenizedRow());
                        }

                        try {
                            RegisterType registerType = RegisterType.valueOf(((String)record.get(0)).trim());
                            XPath path = XPathFactory.newInstance().newXPath();
                            path.setNamespaceContext(new NamespaceContext() {
                                @Override
                                public String getNamespaceURI(String s) {
                                    if (NAMESPACE_MAPPING.equals(s)) return Main.XML_NAMESPACE;
                                    return XMLConstants.NULL_NS_URI;
                                }

                                @Override
                                public String getPrefix(String s) { throw new UnsupportedOperationException(); }
                                @Override
                                public Iterator getPrefixes(String s) { throw new UnsupportedOperationException(); }
                            });
                            String xpathStr = ((String)record.get(1)).trim();

//                            xpathStr = xpathStr.replace("/", "/" + NAMESPACE + ":");
//                            xpathStr = xpathStr.replaceAll("(/@|/)", "$1" + NAMESPACE + ":");
                            xpathStr = xpathStr.replaceAll("/(?!@)", "/" + NAMESPACE_MAPPING + ":");
                            XPathExpression xpath = path.compile(xpathStr);
                            boolean flattenArray = "true".equalsIgnoreCase(this.trim((String)record.get(2)));
                            String jsonName = ((String)record.get(3)).trim();
                            List<Mode> modes = new LinkedList<>();
                            if ("true".equalsIgnoreCase(this.trim((String)record.get(4)))) {
                                modes.add(Mode.CHANGES);
                            }

                            if ("true".equalsIgnoreCase(this.trim((String)record.get(5)))) {
                                modes.add(Mode.CONSOLIDATED_UPSERT);
                            }

                            if ("true".equalsIgnoreCase(this.trim((String)record.get(6)))) {
                                modes.add(Mode.CONSOLIDATED_DELETE);
                            }

                            this.allTransformRules.add(new TransformRule(registerType, xpath, jsonName, flattenArray, modes));
                        } catch (Exception var60) {
                            throw new IllegalArgumentException("Chyba pri parsovani riadku konfiguracie. Definicia riadku je pravdepodobne nekorektna: " + listReader.getUntokenizedRow(), var60);
                        }
                    }
                } catch (Throwable var61) {
                    var7 = var61;
                    throw var61;
                } finally {
                    if (var7 != null) {
                        try {
                            listReader.close();
                        } catch (Throwable var59) {
                            var7.addSuppressed(var59);
                        }
                    } else {
                        listReader.close();
                    }

                }
            } catch (Throwable var63) {
                var5 = var63;
                throw var63;
            } finally {
                if (var5 != null) {
                    try {
                        reader.close();
                    } catch (Throwable var58) {
                        var5.addSuppressed(var58);
                    }
                } else {
                    reader.close();
                }
            }
        } catch (Throwable var65) {
            var3 = var65;
            try {
                throw var65;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            if (var3 != null) {
                try {
                    is.close();
                } catch (Throwable var57) {
                    var3.addSuppressed(var57);
                }
            } else {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String trim(String value) {
        return value == null ? "" : value.trim();
    }


    public void transformXmlToJson(File file, Registers.Register register) {
        try (FileInputStream is = new FileInputStream(file);
             Reader xmlReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            transformAndSave(xmlReader, register);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *  method that transforms large initial batches effectively, writes results straight to file specified inside
     *  creates a folder for each register in which it stores transformed chunks of initial batch
     */
    private void transformAndSave(Reader xmlReader, Registers.Register register) throws IOException {
        try {
            System.out.println("Going to transform XML initial batch " + register.getRegisterType().toString());
            Mode mode = Mode.CONSOLIDATED_UPSERT;
            RegisterType registerType = register.getRegisterType();
            String changeElementName = registerType.changeElementName;
            ArrayList<TransformRule> transformRules = new ArrayList<>();

            for (TransformRule tr : this.allTransformRules) {
                if (tr.getModes().contains(mode) && tr.getRegisterType().equals(registerType)) {
                    transformRules.add(tr);
                }
            }
            String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date());

            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            JsonBuilderFactory jsonFactory = Json.createBuilderFactory(Collections.<String, Object>emptyMap());
            XPathFactory xpathFactory = XPathFactory.newInstance();

            JsonArrayBuilder jsonArrayBuilder = jsonFactory.createArrayBuilder();
            XMLEventReader xer;
            BufferedWriter writer;
            File dir = new File(Main.INIT_JSON_DIRECTORY_PATH + register.getRegisterType().name());
            if(!dir.exists()) {
                if(!dir.mkdir()){
                    throw new RuntimeException("Failed to create a destination directory for JSON initial batch for register " + register.getRegisterType().name());
                }
            }

            xer = xmlInputFactory.createXMLEventReader(xmlReader);
            Document doc;

            int i = 0;
            int chunkCount = 0;
            writer = new BufferedWriter(new FileWriter(new File(Main.INIT_JSON_DIRECTORY_PATH +
                    register.getRegisterType().name() + "/" + chunkCount + ".json")));

            while(true){
                doc = XmlFragmentExtractor.getNextFragment(xer, changeElementName);
                if(doc == null) break;

                JsonObject item = this.transformElementToJson(doc, mode, transformRules, modifiedTimestamp,jsonFactory);
                jsonArrayBuilder.add(item);
                i++;
                if(i >= MAX_CHUNK_SIZE){
                    chunkCount++;
                    System.out.println("Saving chunk " + chunkCount + " . . .");
                    i = 0;
                    String jsonChunk = jsonArrayBuilder.build().toString();
                    writer.write(jsonChunk);
                    writer.close();

                    writer = new BufferedWriter(new FileWriter(new File(Main.INIT_JSON_DIRECTORY_PATH +
                            register.getRegisterType().name() + "/" + chunkCount + ".json")));
                    jsonArrayBuilder = jsonFactory.createArrayBuilder();
                }
            }

            xer.close();
            System.out.println("Saving last chunk.");
            String jsonChunk = jsonArrayBuilder.build().toString();
            writer.write(jsonChunk);
            writer.close();

            System.out.println("XML to JSON transformation was successful");
        } catch (XMLStreamException | ParserConfigurationException | XPathExpressionException e){
            throw new RuntimeException("Exception while parsing XML initial batch file for register " +
                    register.getRegisterType().name() + ": " + e.toString() + "\n The provided XML file has invalid format.");
        }
    }


    private JsonObject transformElementToJson(Document doc, Mode mode, ArrayList<TransformRule> transformRules, String modifiedTimestamp, JsonBuilderFactory jsonFactory) throws XPathExpressionException {
        JsonObjectBuilder itemBuilder = jsonFactory.createObjectBuilder();

        for (TransformRule transformRule : transformRules) {
            XPathExpression xpath = transformRule.getXpath();

            NodeList nodelist = (NodeList) xpath.evaluate(doc, XPathConstants.NODESET);
            String result;

            if (nodelist.getLength() == 0) {
                result = null;
            } else {
                result = nodelist.item(0).getTextContent();
                if (nodelist.getLength() > 1) {
                    if (!transformRule.isFlattenArray()) {
                        throw new IllegalArgumentException("pre element/atribut, ktory nie je definovany ako array, bol xpathom vrateny viac ako 1 node. Zrejme invalidny vstup, kedze podla XML schemy by takyto pripad nemal nastat.");
                    }

                    for (int i = 1; i < nodelist.getLength(); ++i) {
                        result = result + "," + nodelist.item(i).getTextContent();
                    }
                }
            }

            if (result != null) {
                String jsonName = transformRule.getJsonName();
                if (isTimestampAttribute(jsonName)) {
//                    System.out.println("attr: " + jsonName);
//                    System.out.println("  before: " + result);
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    ZonedDateTime date = ZonedDateTime.parse(result);
                    result = date.format(dtf);
//                    System.out.println("   after: " + result);
//                    date.format(new DateTimeFormatter())
                } else if (isDateAttribute(jsonName)) {
//                    System.out.println("attr: " + jsonName);
//                    System.out.println("  before: " + result);

                    result = result.replaceAll("\\+\\d+:\\d+", "");
//                    System.out.println("   after: " + result);
                }

                itemBuilder.add(transformRule.getJsonName(), result);
            }
        }

        if (mode.equals(Mode.CHANGES)) {
            itemBuilder.add("modified_timestamp", modifiedTimestamp);
        }

        return itemBuilder.build();
    }
    private static final String[] TIMESTAMP_ATTRIBUTES = {"validFrom", "validTo", "changedAt"};
    public static boolean isTimestampAttribute(String jsonName) {
        for (String attr : TIMESTAMP_ATTRIBUTES) {
            if (attr.equalsIgnoreCase(jsonName)) {
                return true;
            }
        }
        return false;
    }

    private static final String[] DATE_ATTRIBUTES = {"effectiveDate", "verifiedAt"};
    public static boolean isDateAttribute(String jsonName) {
        for (String attr : DATE_ATTRIBUTES) {
            if (attr.equalsIgnoreCase(jsonName)) {
                return true;
            }
        }
        return false;
    }

    public enum Mode {
        CHANGES,
        CONSOLIDATED_UPSERT,
        CONSOLIDATED_DELETE;
    }

    public enum RegisterType {
        REGION("regionChange"),
        COUNTY("countyChange"),
        MUNICIPALITY("municipalityChange"),
        DISTRICT("districtChange"),
        STREET_NAME("streetNameChange"),
        PROPERTY_REGISTRATION_NUMBER("propertyRegistrationNumberChange"),
        BUILDING_NUMBER("buildingNumberChange"),
        BUILDING_UNIT("buildingUnitChange");

        public String changeElementName;

        RegisterType(String changeElementName) {
            this.changeElementName = changeElementName;
        }
    }
}
