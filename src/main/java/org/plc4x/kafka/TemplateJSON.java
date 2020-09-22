package org.plc4x.kafka;

public class TemplateJSON {

    String filledTemplate;

    // Constructor
    public TemplateJSON() {
        /*
        filledTemplate = getFilledTemplate();
       */
    }


    public String getFilledTemplate() {
        return filledTemplate;
    }

    public void setFilledTemplate(String filledTemplate) {
        this.filledTemplate = filledTemplate;
    }

    // Fill the whole value template
    //public String fillTemplate(String pModel,String pSourceName,String pAddress,long pTimestamp, String pValues){
    public String fillTemplate(String pModel,String pSourceName,String pAddress,java.sql.Timestamp pTimestamp, String pValues){
        String template;
        template = "{\"model\":\"" + pModel + "\",\"sourceName\":\"" + pSourceName + "\",\"address\":\"" + pAddress +
            "\",\"timestamp\":\"" + pTimestamp + "\",\"values\":[" + pValues + "]}";

        filledTemplate = template;
        return template;
    }





}
