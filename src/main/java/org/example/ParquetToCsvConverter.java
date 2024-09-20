package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ParquetToCsvConverter {
    public static void main(String[] args) {
        String inputParquetFile = "C:\\Users\\ravib\\Downloads\\House_price.parquet";
        String outputCsvFile = "C:\\Users\\ravib\\Downloads\\output.csv";

        try {
            Configuration conf = new Configuration();
            Path path = new Path(inputParquetFile);
            ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();

            ParquetReader<Group> parquetReader = ParquetReader.builder(new GroupReadSupport(), path)
                    .withConf(conf)
                    .build();

            FileWriter csvWriter = new FileWriter(outputCsvFile);

            // Write CSV header
            for (int i = 0; i < fields.size(); i++) {
                csvWriter.append(fields.get(i).getName());
                if (i < fields.size() - 1) {
                    csvWriter.append(",");
                }
            }
            csvWriter.append("\n");

            // Write data
            Group group;
            while ((group = parquetReader.read()) != null) {
                for (int i = 0; i < fields.size(); i++) {
                    String value = group.getValueToString(i, 0);
                    csvWriter.append(value.replace(",", "\\,"));  // Escape commas in the value
                    if (i < fields.size() - 1) {
                        csvWriter.append(",");
                    }
                }
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
            parquetReader.close();
            reader.close();

            System.out.println("Conversion completed successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}