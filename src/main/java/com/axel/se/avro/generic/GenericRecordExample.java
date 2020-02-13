package com.axel.se.avro.generic;

import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;

public class GenericRecordExample {

  public static void main(String[] args) {

    // step 1 create generic record
    CustomerAvroBuilder cab = new CustomerAvroBuilder();
    Record customerAvroRecord = cab.createAxelCustomerAvroRecord();

    System.out.println(customerAvroRecord);
    // step 2 write generic record to file
    final GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(cab.getSchema());
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(genericDatumWriter)) {
      dataFileWriter.create(cab.getSchema(),new File("customer-generic.avro"));
      dataFileWriter.append(customerAvroRecord);
      System.out.printf("Writeing my customer to a file!");
    } catch (IOException e) {
      e.printStackTrace();
      System.out.printf("Houston we have a problem!");
    }

    final File file = new File("customer-generic.avro");
    final GenericDatumReader<Record> genericDatumReader = new GenericDatumReader<>(cab.getSchema());

    try (DataFileReader<Record> recordDataFileReader = new DataFileReader<>(file, genericDatumReader)) {
      // Read next record...
      Record readCustomerRecord = recordDataFileReader.next();
      System.out.println(readCustomerRecord.toString());
      // prints name
      System.out.println("First name" + readCustomerRecord.get("first_name"));
      // returns null!
      System.out.println("?? " + readCustomerRecord.get("cool_nickname"));



    } catch (IOException e) {
      e.printStackTrace();
    }

    // step 3 read generic record from a file
    // step 4 interpret as a generic record

  }

}
