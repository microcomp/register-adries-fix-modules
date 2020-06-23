package ra.transform;

import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Copied from ra_ws_muleproccess project.
 */
public class XmlFragmentExtractor {
    private static XMLOutputFactory xof = XMLOutputFactory.newInstance();
    private static DocumentBuilder docBuilder;

    public XmlFragmentExtractor() {
    }

    public static Document getNextFragment(XMLEventReader xer, String fragmentLocalName) throws XMLStreamException, ParserConfigurationException {
        while(true) {
            if (xer.hasNext()) {
                XMLEvent event = xer.nextEvent();
                if (!event.isStartElement() || !event.asStartElement().getName().getLocalPart().equals(fragmentLocalName)) {
                    continue;
                }

                return extractCurrentElement(xer, event);
            }

            return null;
        }
    }

    public static Document extractCurrentElement(XMLEventReader xer, XMLEvent startElementEvent) throws XMLStreamException, ParserConfigurationException {
        String fragmentLocalName = startElementEvent.asStartElement().getName().getLocalPart();
        Document doc = docBuilder.newDocument();
        DOMResult domResult = new DOMResult(doc);
        XMLEventWriter xew = null;

        try {
            xew = xof.createXMLEventWriter(domResult);
            xew.add(startElementEvent);
            int depth = 0;

            while(true) {
                if (xer.hasNext()) {
                    XMLEvent innerEvent = xer.nextEvent();
                    xew.add(innerEvent);
                    if (innerEvent.isStartElement()) {
                        ++depth;
                        continue;
                    }

                    if (!innerEvent.isEndElement()) {
                        continue;
                    }

                    --depth;
                    if (!innerEvent.asEndElement().getName().getLocalPart().equals(fragmentLocalName) || depth != -1) {
                        continue;
                    }
                }

                xew.flush();
                Document var16 = (Document)domResult.getNode();
                return var16;
            }
        } finally {
            if (xew != null) {
                try {
                    xew.close();
                } catch (XMLStreamException ignored) {
                }
            }

        }
    }

    private static String nodeToString(Node node) {
        StringWriter sw = new StringWriter();

        try {
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.setOutputProperty("omit-xml-declaration", "yes");
            t.setOutputProperty("indent", "yes");
            t.transform(new DOMSource(node), new StreamResult(sw));
        } catch (TransformerException var3) {
            System.out.println("nodeToString Transformer Exception");
        }

        return sw.toString();
    }

    static {
        try {
            docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException var1) {
            throw new RuntimeException("Neocakavana chyba pri inicializacii", var1);
        }
    }
}
