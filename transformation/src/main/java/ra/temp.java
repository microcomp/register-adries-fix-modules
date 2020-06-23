package ra;

import ra.transform.XmlToJsonTransformer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class temp {
    public static void main(String[] args) throws Exception {
        String modifiedTimestamp = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date());
        System.out.println(modifiedTimestamp);
        Main.INIT_JSON_DIRECTORY_PATH = "/home/boris/Desktop/microcomp_2020/init_batch/test/json/";
        XmlToJsonTransformer transformer = new XmlToJsonTransformer();
        Registers registers = new Registers(true);
        for(Registers.Register register : registers.getAllRegisters()){
            File initBatch = new File("/home/boris/Desktop/microcomp_2020/init_batch/xml/DavkaInit_20200110_edov2ra-01.xml");
            transformer.transformXmlToJson(initBatch, register);
        }

    }
}
