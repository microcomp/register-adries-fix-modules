package ra.transform;

import java.util.Collections;
import java.util.List;
import javax.xml.xpath.XPathExpression;
import ra.transform.XmlToJsonTransformer.Mode;
import ra.transform.XmlToJsonTransformer.RegisterType;


/**
 * Copied from ra_ws_muleproccess project.
 */
public class  TransformRule {
    private final RegisterType registerType;
    private final XPathExpression xpath;
    private final boolean flattenArray;
    private final String jsonName;
    private final List<Mode> modes;

    public TransformRule(RegisterType registerType, XPathExpression xpath, String jsonName, boolean flattenArray, List<Mode> modes) {
        this.xpath = xpath;
        this.jsonName = jsonName;
        this.flattenArray = flattenArray;
        this.modes = Collections.unmodifiableList(modes);
        this.registerType = registerType;
    }

    public XPathExpression getXpath() {
        return this.xpath;
    }

    public boolean isFlattenArray() {
        return this.flattenArray;
    }

    public String getJsonName() {
        return this.jsonName;
    }

    public List<Mode> getModes() {
        return this.modes;
    }

    public RegisterType getRegisterType() {
        return this.registerType;
    }
}

