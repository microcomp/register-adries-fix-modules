package ra;

import ra.transform.XmlToJsonTransformer;
import ra.transform.XmlToJsonTransformer.RegisterType;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Holds all the necessary data specific to each register.
 */
public class Registers {
    private List<Register> registers;

    public Registers() {
        registers = new ArrayList<>();
        registers.add(new Register(RegisterType.REGION, "01"));
        registers.add(new Register(RegisterType.COUNTY, "02"));
        registers.add(new Register(RegisterType.MUNICIPALITY, "03"));
        registers.add(new Register(RegisterType.DISTRICT, "04"));
        registers.add(new Register(RegisterType.STREET_NAME, "05"));
        registers.add(new Register(RegisterType.BUILDING_UNIT, "08"));
        registers.add(new Register(RegisterType.PROPERTY_REGISTRATION_NUMBER, "06"));
        registers.add(new Register(RegisterType.BUILDING_NUMBER, "07"));
    }

    public Registers(boolean test) {
        registers = new ArrayList<>();
        registers.add(new Register(RegisterType.REGION, "01"));
//        registers.add(new Register(RegisterType.COUNTY, "02"));
//        registers.add(new Register(RegisterType.MUNICIPALITY, "03"));
//        registers.add(new Register(RegisterType.DISTRICT, "04"));
//        registers.add(new Register(RegisterType.STREET_NAME, "05"));
//        registers.add(new Register(RegisterType.BUILDING_UNIT, "08"));
//        registers.add(new Register(RegisterType.PROPERTY_REGISTRATION_NUMBER, "06"));
//        registers.add(new Register(RegisterType.BUILDING_NUMBER, "07"));
    }


    public List<Register> getAllRegisters() {
        return registers;
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

    public static class Register {
        private final RegisterType registerType;
        private final String fileSuffix;
        private String xmlFileName;

        private Register(RegisterType registerType, String fileSuffix) {
            this.registerType = registerType;
            this.fileSuffix = fileSuffix;
        }

        public XmlToJsonTransformer.RegisterType getRegisterType() {
            return registerType;
        }

        public String getFileSuffix() {
            return fileSuffix;
        }

        public String getXmlFileName() {
            return xmlFileName;
        }

        public void setXmlFileName(String xmlFileName) {
            this.xmlFileName = xmlFileName;
        }
    }

}
