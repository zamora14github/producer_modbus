package org.plc4x.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.scraper.config.triggeredscraper.JobConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImpl;
import org.apache.plc4x.java.scraper.config.triggeredscraper.ScraperConfigurationTriggeredImplBuilder;
import org.apache.plc4x.java.scraper.exception.ScraperException;
import org.apache.plc4x.java.scraper.triggeredscraper.TriggeredScraperImpl;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollector;
import org.apache.plc4x.java.scraper.triggeredscraper.triggerhandler.collector.TriggerCollectorImpl;
import org.apache.plc4x.java.utils.connectionpool.PooledPlcDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;


public class SimpleApp_modbus {



    private static final Logger logger = LoggerFactory.getLogger(SimpleApp_modbus.class);

    static String[] Modbus_addresses;
    static HashMap<String, String> source_ip_modbus;
    static HashMap<String, String> source_model_modbus;


    public static void main(String[] args) {

        // Create addresses by model
        Modbus_addresses = createAddressesArray_Modbus();


        // Initializate HashMaps
        source_ip_modbus = generateSourceIp_modbus();
        source_model_modbus = generateSourceModel_modbus();


        // Create Kafka Producer for the Modbus
        KafkaProducer<String, String> producer2 = new Producer().getProducer();


        // Scrapper Configuration
        ScraperConfigurationTriggeredImplBuilder scrapperBuilderModbus = new ScraperConfigurationTriggeredImplBuilder();


        // Add Modbus sources (at least one)
        for(int i=0; i<Modbus_addresses.length; i++){
            scrapperBuilderModbus.addSource("Modbus_SourcePLC" + i, Modbus_addresses[i]);
        }

        // In order to configure a job we have to get an instance of a JobConfigurationTriggeredImplBuilder.
        JobConfigurationTriggeredImplBuilder Modbus_jobBuilder = scrapperBuilderModbus.job("Modbus_Job","(SCHEDULED,100)");

        // Assign sources to a job
        for(int j=0; j<Modbus_addresses.length; j++) {
            Modbus_jobBuilder.source("Modbus_SourcePLC" + j);
        }




        // Modbus fields
        //PlcReadRequest.Builder builder = plcConnection.readRequestBuilder();
        Modbus_jobBuilder.field("coil1", "coil:1");
        //Modbus_jobBuilder.field("coil3.4", "coil:3[4]");
        Modbus_jobBuilder.field("holdingRegister1", "holding-register:1");
        //Modbus_jobBuilder.field("holdingRegister3.4", "holding-register:3[4]");
        Modbus_jobBuilder.build();
        ScraperConfigurationTriggeredImpl Modbus_scraperConfig = scrapperBuilderModbus.build();




        // To RUN the Modbus_Scrapper ----------------
        try {
            // Create a new PooledPlcDriverManager
            PlcDriverManager Modbus_plcDriverManager = new PooledPlcDriverManager();
            // Trigger Collector
            TriggerCollector Modbus_triggerCollector = new TriggerCollectorImpl(Modbus_plcDriverManager);

            // Messages counter
            //AtomicInteger messagesCounter2 = new AtomicInteger();

            // Configure the scraper, by binding a Scraper Configuration, a ResultHandler and a TriggerCollector together
            TriggeredScraperImpl Modbus_scraper = new TriggeredScraperImpl(Modbus_scraperConfig, (jobName, sourceName, results) -> {
                LinkedList<Object> Modbus_results = new LinkedList<>();

                //messagesCounter2.getAndIncrement();

                Modbus_results.add(jobName);
                Modbus_results.add(sourceName);
                Modbus_results.add(results);

                logger.info("Array: " + String.valueOf(Modbus_results));
                //logger.info("MESSAGE_MODBUS number: " + messagesCounter2);

                // Producer topics routing
                String topic = "modbus" + Modbus_results.get(1).toString().substring(Modbus_results.get(1).toString()
                        .indexOf("Modbus_SourcePLC") + 13 , Modbus_results.get(1).toString().length());
                String key = parseKey_Modbus("modbus");
                String value = parseValue_Modbus(Modbus_results.getLast().toString(),Modbus_results.get(1).toString());
                logger.info("------- PARSED MODBUS VALUE -------------------------------- " + value);



                ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(topic, key, value);

                // Send Data to Kafka - asynchronous
                producer2.send(record2, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                });

            }, Modbus_triggerCollector);


            Modbus_scraper.start();
            Modbus_triggerCollector.start();
        } catch (ScraperException e) {
            logger.error("Error starting the scraper (Modbus_scrapper)", e);
        }


    }




    private static String parseKey_Modbus(String kValue){
        String parsedString = kValue.replace("modbus", "{\"protocol\":\"modbus\"}");
        return parsedString;
    }

    private static String parseValue_Modbus(String vValue, String pSourceName){
        String partialParsedString = vValue.replace("=[", ":\"").replace("]", "\"")
                .replace("=", ":")
                .replaceAll("coil1", "\"coil1\"")
                .replaceAll("holdingRegister1", "\"holdingRegister1\"");
                //.replaceAll("coil3.4","\"coil3.4\"")
                //.replaceAll("holdingRegister3.4", "\"holdingRegister3.4\"");

        TemplateJSON template2 = new TemplateJSON();
        String modbus_parsed = template2.fillTemplate(source_model_modbus.get(pSourceName),pSourceName, source_ip_modbus.get(pSourceName)
                ,getTimestamp(),partialParsedString);
       return modbus_parsed;
    }



    static Timestamp getTimestamp(){
        Calendar calendar = Calendar.getInstance();
        Timestamp ourJavaTimestampObject = new Timestamp(calendar.getTime().getTime());

        return ourJavaTimestampObject;
    }





    // -----   Create 2 Maps (To pass the parameters to the TemplateJSON) ----------------------------
    // 1- Relations {sourceName - IP} for the modbus
    static HashMap<String,String> generateSourceIp_modbus(){
        source_ip_modbus = new HashMap<String, String>();

        for(int i=0; i< Modbus_addresses.length; i++){
            source_ip_modbus.put("Modbus_SourcePLC" + i,Modbus_addresses[i].substring(13));
        }
        return source_ip_modbus;
    }

    // 2- Relations {sourceName - model} for the modbus
    static HashMap<String,String> generateSourceModel_modbus(){
        source_model_modbus = new HashMap<String, String>();

        for(int i=0; i<Modbus_addresses.length; i++){
            source_model_modbus.put("Modbus_SourcePLC" + i,"Modicon TSX Quantum 140");
        }

        return source_model_modbus;
    }



    private static String[] createAddressesArray_Modbus(){
        // Create the Array
        String[] modbusAdresses = {"modbus:tcp://10.172.19.222"};
        return modbusAdresses;
    }


}
