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
import java.util.stream.Collectors;

public class ParquetToCsvConverter {
    public static void main(String[] args) {
        String inputParquetFile = "path/to/your/input.parquet";
        String outputCsvFile = "path/to/your/output.csv";
        String columnToIgnore = "fileMetadata";

        try {
            Configuration conf = new Configuration();
            Path path = new Path(inputParquetFile);
            ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf));
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Type> fields = schema.getFields().stream()
                    .filter(field -> !field.getName().equals(columnToIgnore))
                    .collect(Collectors.toList());

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
                int fieldIndex = 0;
                for (int i = 0; i < schema.getFields().size(); i++) {
                    if (!schema.getFields().get(i).getName().equals(columnToIgnore)) {
                        String value = group.getValueToString(i, 0);
                        csvWriter.append(value.replace(",", "\\,"));  // Escape commas in the value
                        if (fieldIndex < fields.size() - 1) {
                            csvWriter.append(",");
                        }
                        fieldIndex++;
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