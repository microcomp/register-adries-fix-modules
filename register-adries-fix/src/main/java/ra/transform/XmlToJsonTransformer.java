package ra.transform;

import java.io.*;
import java.io.BufferedReader;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import ra.Registers;

/**
 * Copied from ra_ws_muleproccess project.
 * Minor optimizations have been made.
 */
public class XmlToJsonTransformer {
    private static final String CONFIGURATION_CSV_FILE_PATH = "src/main/resources/xml_to_json_transform_config.csv";
    private static final int CONFIGURATION_COLUMN_COUNT = 7;
    private static final int MAX_CHUNK_SIZE = 40000; //up to 20 MB
    private static final String SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private ArrayList<TransformRule> allTransformRules = new ArrayList<>();

    public XmlToJsonTransformer() throws Exception {
        this.initFromCsv();
    }

    private void initFromCsv() throws FileNotFoundException {
        InputStream is = getClass().getResourceAsStream("/xml_to_json_transform_config.csv");
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
                            String xpathStr = ((String)record.get(1)).trim();
                            XPathExpression xpath = XPathFactory.newInstance().newXPath().compile(xpathStr);
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


    public JsonArray transformXmlToJson(File file, Registers.Register register, String modeAsString) throws XMLStreamException, XPathExpressionException, ParserConfigurationException, IOException {

        FileInputStream is = new FileInputStream(file);
        Reader xmlReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        JsonArray jsonArray = transformXmlToJson(xmlReader, register, modeAsString);
        is.close();
        xmlReader.close();
        return jsonArray;
    }



    private JsonArray transformXmlToJson(Reader xmlReader, Registers.Register register, String modeAsString) throws XMLStreamException, ParserConfigurationException, XPathExpressionException {
        XmlToJsonTransformer.Mode mode = XmlToJsonTransformer.Mode.valueOf(modeAsString);
        XmlToJsonTransformer.RegisterType registerType = register.getRegisterType();
        String changeElementName = registerType.changeElementName;
        ArrayList<TransformRule> transformRules = new ArrayList<>();

        for (TransformRule tr : this.allTransformRules) {
            if (tr.getModes().contains(mode) && tr.getRegisterType().equals(registerType)) {
                transformRules.add(tr);
            }
        }

//        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).format(new Date());
        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date());
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        JsonBuilderFactory jsonFactory = Json.createBuilderFactory(Collections.emptyMap());
        XPathFactory xpathFactory = XPathFactory.newInstance();
        JsonArrayBuilder jsonArrayBuilder = jsonFactory.createArrayBuilder();
        XPathExpression dbOperationXpath = xpathFactory.newXPath().compile("/" + changeElementName + "/databaseOperation");
        XMLEventReader xer = null;

        try {
            xer = xmlInputFactory.createXMLEventReader(xmlReader);
            Document doc;
            while(true){
                doc = XmlFragmentExtractor.getNextFragment(xer, changeElementName);
                if(doc == null) break;

                JsonObject item = this.transformElementToJson(doc, mode, transformRules, modifiedTimestamp,jsonFactory);
                jsonArrayBuilder.add(item);
            }
        } finally {
            if (xer != null) {
                try {
                    xer.close();
                } catch (XMLStreamException ignored) {
                }
            }
        }

        JsonArray resultArray = jsonArrayBuilder.build();
        return resultArray;
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
                        throw new IllegalArgumentException("pre element/atribut, ktory nie je definovany ako array, bol xpathom vrateny viac ako 1 node!Zrejme invalidny vstup, kedze podla XML schemy by takyto pripad nemal nastat.");
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
                    LocalDateTime date = LocalDateTime.parse(result);
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    result = date.format(dtf);
//                    System.out.println("   after: " + result);

                } else if ("verifiedAt".equalsIgnoreCase(jsonName)) {
//                    System.out.println("attr: " + jsonName);
//                    System.out.println("  before: " + result);
                    LocalDateTime date = LocalDateTime.parse(result);
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    result = date.format(dtf);
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


    public enum ChangeElementOperation {
        INSERT,
        UPDATE,
        DELETE;

        private ChangeElementOperation() {
        }
    }

    public enum Mode {
        CHANGES,
        CONSOLIDATED_UPSERT,
        CONSOLIDATED_DELETE;

        private Mode() {
        }
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

        private RegisterType(String changeElementName) {
            this.changeElementName = changeElementName;
        }
    }
}
