package ra;

import ra.transform.XmlToJsonTransformer;
import ra.transform.XmlToJsonTransformer.RegisterType;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Holds all of the necessary data specific to each register.
 */
public class Registers {
    private List<Register> registers;

    public Registers() {
        registers = new ArrayList<>();
        registers.add(new Register(RegisterType.REGION, "register-krajov","Register krajov", "Kraje", "01"));
        registers.add(new Register(RegisterType.COUNTY, "register-okresov","Register okresov", "Okresy", "02"));
        registers.add(new Register(RegisterType.MUNICIPALITY, "register-obci","Register obcí", "Obce","03"));
        registers.add(new Register(RegisterType.DISTRICT, "register-casti-obci","Register častí obcí", "Časti obcí","04"));
        registers.add(new Register(RegisterType.STREET_NAME, "register-ulic", "Register ulíc", "Ulice","05"));
        registers.add(new Register(RegisterType.PROPERTY_REGISTRATION_NUMBER, "register-budov","Register budov (súpisných čísiel)","Budovy", "06"));
        registers.add(new Register(RegisterType.BUILDING_NUMBER,"register-vchodov", "Register vchodov (orientačných čísiel)", "Vchody", "07"));
        registers.add(new Register(RegisterType.BUILDING_UNIT, "register-bytov", "Register bytov", "Byty", "08"));
    }

    /* temporary constructor created for testing purposes */
    public Registers(boolean test){
        registers = new ArrayList<>();
        registers.add(new Register(RegisterType.STREET_NAME, "register-ulic", "Register ulíc", "Ulice","05"));
//        registers.add(new Register(RegisterType.REGION, "register-krajov","Register krajov", "Kraje", "01"));
//        registers.add(new Register(RegisterType.MUNICIPALITY, "register-obci","Register obcí", "Obce","03"));
//        registers.add(new Register(RegisterType.DISTRICT, "register-casti-obci","Register častí obcí", "Časti obcí","04"));
//        registers.add(new Register(RegisterType.COUNTY, "register-okresov","Register okresov", "Okresy", "02"));
//        registers.add(new Register(RegisterType.PROPERTY_REGISTRATION_NUMBER, "register-budov","Register budov (súpisných čísiel)","Budovy", "06"));
//        registers.add(new Register(RegisterType.BUILDING_NUMBER,"register-vchodov", "Register vchodov (orientačných čísiel)", "Vchody", "07"));
//        registers.add(new Register(RegisterType.BUILDING_UNIT, "register-bytov", "Register bytov", "Byty", "08"));
    }


    /**
     * inner class that represents the individual register and its data
     *
     * PRIMARY_KEYS - list of table columns that are used as unique key when updating records in a datastore
     * registerType - corresponding enum value used when transforming initial batches / changes batches
     * title - the Slovak title of the register in Register Adries database
     * name - used instead of package id when accessing a dataset
     * resourceNameBase - abbreviation of the entire dataset name used for example as a prefix for individual resources
     * fileSuffix - used to determine the appropriate initial batch, numerical value ("01" - "08")
     * changedTableFields - names of the datastore columns mapped to their datatype for 'changed' datastore
     * consolidatedTableFields - names of the datastore columns mapped to their datatype 'consolidated' datastore
     * changedId - id of the 'changed' resource, will be set manually when working with DATA.GOV database
     * consolidatedId - id of the 'consolidated' resource, will be set manually when working with DATA.GOV database
     */
    public class Register {
        private final String[] PRIMARY_KEYS = {"versionId"};
        private final RegisterType registerType;
        private final String title;
        private final String name;
        private final String resourceNameBase;
        private final String fileSuffix;
        private final Map<String, String> changedTableFields;
        private final Map<String, String> consolidatedTableFields;
        private String xmlFileName;
        private String docFileName;
        private String changedId;
        private String consolidatedId;
        private String thirdId;

        private Register(XmlToJsonTransformer.RegisterType registerType,
                        String nameSuffix, String titleSuffix, String resourceNameBase, String fileSuffix) {
            this.registerType = registerType;
            this.name = "register-adries-" +  nameSuffix;
            this.title = "Register Adries - " + titleSuffix;
            this.resourceNameBase = resourceNameBase;
            this.fileSuffix = fileSuffix;
            this.consolidatedTableFields = generateFieldsMap(registerType);
            this.changedTableFields = generateFieldsMap(registerType);
            /* for now the only extra column that is contained in the 'changed' datastore, might change */
            this.changedTableFields.put("modified_timestamp", "timestamp");
        }

        public void setChangedId(String changedId) {
            this.changedId = changedId;
        }

        public void setConsolidatedId(String consolidatedId) {
            this.consolidatedId = consolidatedId;
        }

        public XmlToJsonTransformer.RegisterType getRegisterType() {
            return registerType;
        }

        public String getChangedId() {
            return changedId;
        }

        public String getConsolidatedId() {
            return consolidatedId;
        }

        public String getThirdId() {
            return thirdId;
        }

        public void setThirdId(String thirdId) {
            this.thirdId = thirdId;
        }

        public Map<String, String> getChangedTableFields() {
            return changedTableFields;
        }
        public Map<String, String> getConsolidatedTableFields(){
            return consolidatedTableFields;
        }
        public String getTitle() {
            return title;
        }
        public String getName(){
            return name;
        }

        public String getChangedResourceDescription() {
            if (this.registerType == RegisterType.STREET_NAME)
                return "Jeden riadok v tejto tabuľke reprezentuje jeden zmenový záznam streetNameChange " +
                        "(zmena názvu ulice), ktorý prišiel v zmenovej dávke z Registra adries. Celá tabuľka" +
                        " tak obsahuje históriu všetkých zmien v Registri adries pre všetky ulice.";
            else if (this.registerType == RegisterType.BUILDING_NUMBER)
                return "Jeden riadok v tejto tabuľke reprezentuje jeden zmenový záznam buildingNumberChange " +
                        "(zmena vchodu [orientačného čísla]), ktorý prišiel v zmenovej dávke z Registra adries." +
                        " Celá tabuľka tak obsahuje históriu všetkých zmien v Registri adries pre všetky vchody (orientačné čísla).";
            return ""; // not needed
        }
        public String getConsolidatedResourceDescription() {
            if (this.registerType == RegisterType.STREET_NAME)
                return "Jeden riadok v tejto tabuľke reprezentuje konsolidovaný stav konkrétnej časovej verzie objektu" +
                        " Ulica. Celá tabuľka tak obsahuje všetky časové verzie všetkých Ulíc - aktuálne aj historické.";
            else if (this.registerType == RegisterType.BUILDING_NUMBER)
                return "Jeden riadok v tejto tabuľke reprezentuje konsolidovaný stav konkrétnej časovej verzie objektu " +
                        "Vchod (orientačné číslo). Celá tabuľka tak obsahuje všetky časové verzie všetkých vchodov " +
                        "(orientačných čísiel) - aktuálne aj historické.";
            return ""; // not needed
        }

        public String getResourceNameBase() {
            return resourceNameBase;
        }

        public String getFileSuffix() {
            return fileSuffix;
        }

        public String[] getPrimaryKeys(){
            return PRIMARY_KEYS;
        }

        public String getXmlFileName() {
            return xmlFileName;
        }

        public String getDocFileName() {
            return docFileName;
        }

        public void setXmlFileName(String xmlFileName) {
            this.xmlFileName = xmlFileName;
        }

        public void setDocFileName(String docFileName) {
            this.docFileName = docFileName;
        }
    }
    public static String getDocResourceDescription() {
        return "Obsahuje popis dátových zdrojov (tabuliek) v tomto datasete.";
    }

    public static String getThirdDSResourceDescription() {
        return "Jeden riadok v tejto tabuľke reprezentuje jeden počiatočný alebo zmenový záznam. Celá " +
                "tabuľka tak obsahuje všetky počiatočné záznamy a históriu všetkých ich zmien v Registri adries.";
    }

    public static String getArchiveResourceDescription() {
        String timestamp = new SimpleDateFormat("MM/yyyy").format(new Date());
        return "ZIP obsahuje archivovaný obsah datasetu Registra adries do " + timestamp + ".";
    }

    public static String getInitBatchResourceDescription() {
        String timestamp = new SimpleDateFormat("MM/yyyy").format(new Date());
        return "Obsahuje xml súbor - konsolidované dáta Registra adries do " + timestamp + ".";
    }

    public List<Register> getAllRegisters() {
        return registers;
    }

    public List<String> getDatasetList(){
        List<String> datasets = new ArrayList<>();
        for(Register register : getAllRegisters()){
            datasets.add(register.name);
        }
        return datasets;
    }

    public Register registerFromFileSuffix(String fileSuffix){
        for(Register register : registers){
            if (register.fileSuffix.equals(fileSuffix)){
                return register;
            }
        }
        throw new RuntimeException("Invalid file suffix string " + fileSuffix + ". Make sure that " +
                "file names follow proper conventions (see instructions for further details).");
    }

    private Map<String, String> generateFieldsMap(RegisterType registerType) {
        Map<String, String> tableFields = new LinkedHashMap<>();
        tableFields.putAll(generateCommonFields());
        tableFields.putAll(generateSpecificFields(registerType));
        return tableFields;
    }

    private Map<String, String> generateSpecificFields(RegisterType registerType) {
        Map<String, String> fields = new LinkedHashMap<>();
        switch(registerType){
            case REGION:
                fields.put("codelistCode", "text");
                fields.put("regionCode", "text");
                fields.put("regionName", "text");
                break;
            case COUNTY:
                fields.put("codelistCode", "text");
                fields.put("countyCode", "text");
                fields.put("countyName", "text");
                fields.put("regionIdentifier", "int8");
                break;
            case MUNICIPALITY:
                fields.put("codelistCode", "text");
                fields.put("municipalityCode", "text");
                fields.put("municipalityName", "text");
                fields.put("countyIdentifier", "int8");
                fields.put("status", "text");
                fields.put("cityIdentifier", "int8");
                break;

            case DISTRICT:
                fields.put("uniqueNumbering", "bool");
                fields.put("codelistCode", "text");
                fields.put("districtCode", "text");
                fields.put("districtName", "text");
                fields.put("municipalityIdentifier", "int8");
                break;
            case STREET_NAME:
                fields.put("streetName", "text");
                fields.put("municipalityIdentifiers", "text");
                fields.put("districtIdentifiers", "text");
                break;
            case BUILDING_UNIT:
                fields.put("unitNumber", "text");
                fields.put("floor", "int8");
                fields.put("buildingNumberIdentifier", "int8");
                break;
            case BUILDING_NUMBER:
                fields.put("buildingNumber", "text");
                fields.put("buildingIndex", "text");
                fields.put("postalCode", "text");
                fields.put("axisB", "float8");
                fields.put("axisL", "float8");
                fields.put("propertyRegistrationNumberIdentifier", "int8");
                fields.put("streetNameIdentifier", "int8");
                fields.put("verifiedAt", "timestamp");
                break;
            case PROPERTY_REGISTRATION_NUMBER:
                fields.put("propertyRegistrationNumber", "int8");
                fields.put("containsFlats", "bool");
                fields.put("buildingPurposeCodelistCode", "text");
                fields.put("buildingName", "text");
                fields.put("buildingPurposeCode", "text");
                fields.put("buildingPurposeName", "text");
                fields.put("buildingTypeCodelistCode", "text");
                fields.put("buildingTypeCode", "int8");
                fields.put("buildingTypeName", "text");
                fields.put("municipalityIdentifier", "int8");
                fields.put("districtIdentifier", "int8");
                break;
        }
        return fields;
    }

    private Map<String, String> generateCommonFields() {
        Map<String, String> commonFields = new LinkedHashMap<>();
        commonFields.put("changeId", "int8");
        commonFields.put("changedAt", "timestamp");
        commonFields.put("databaseOperation", "text");
        commonFields.put("objectId", "int8");
        commonFields.put("versionId", "int8");  //unique (primary) key
        commonFields.put("createdReason", "text");
        commonFields.put("validFrom", "timestamp");
        commonFields.put("validTo", "timestamp");
        commonFields.put("effectiveDate", "date");
        return commonFields;
    }
}
