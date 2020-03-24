package com.santu.kafkapublisher.publisher;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class KafkaCsvProducer {

    private static final String SAMPLE_CSV_FILE_PATH = "C:\\Users\\santhosh\\IdeaProjects\\SampleData_.csv";

    public void publishMessages() {
        JsonParser parser = new JsonParser();
        try (
                Reader reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build();
        ){
            String[] nextRecord;
            batchProcess();
            while ((nextRecord = csvReader.readNext()) != null){
                parser.parseToJson(nextRecord);
            }

        } catch (IOException ex){
            ex.printStackTrace();
        }

    }

    private void batchProcess() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5); // we can change this sizze
        for(int i = 1; i < 5; i++){
            KafkaPush kafkaPush = new KafkaPush("Task "+ i);
            executor.execute(kafkaPush);
        }
        executor.shutdown();
    }
}
