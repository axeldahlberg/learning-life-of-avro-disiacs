package com.axel.se.avro.evolution;

import com.example.CustomerV1;
import com.example.CustomerV2;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SchemaEvolution {

  public static void main(String[] args) {
    backwardsCompability();
    System.out.println("XXXXXXXXXXXXXXXXXXXXXXXX");
    forwardCompability();
  }

  // Writes CustomerV2 reads with CustomerV1 schema
  public static void forwardCompability() {
    CustomerV2 c2 = CustomerV2.newBuilder()
        .setFirstName("Axel")
        .setLastName("Dahlberg")
        .setAge(31)
        .setPhoneNumber("000")
        .setHeight(182F)
        .setWeight(84F)
        .build();

    final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<>(CustomerV2.class);
    final DataFileWriter<CustomerV2> dfw = new DataFileWriter<>(datumWriter);
    try {
      dfw.create(c2.getSchema(),new File("customerV2.avro"));
      dfw.append(c2);
      dfw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    final File file = new File("customerV2.avro");
    final DatumReader<CustomerV1> dr = new SpecificDatumReader<>(CustomerV1.class);
    final DataFileReader<CustomerV1> dfr;
    try {
      dfr = new DataFileReader<CustomerV1>(file,dr);
      while (dfr.hasNext()){
        CustomerV1 cv1 = dfr.next();
        System.out.println("Customer v1" + cv1.toString());
      }
      System.out.println("cool front compability!");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // Writes CustomerV1 reads with CustomerV2 schema
  public static void backwardsCompability() {
    CustomerV1 c1 = CustomerV1.newBuilder()
      .setFirstName("Axel")
      .setLastName("Dahlberg")
      .setAge(31)
      .setAutomatedEmail(false)
      .setHeight(182F)
      .setWeight(84F)
      .build();

    final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
    final DataFileWriter<CustomerV1> dfw = new DataFileWriter<>(datumWriter);
    try {
      dfw.create(c1.getSchema(),new File("customerV1.avro"));
      dfw.append(c1);
      dfw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    final File file = new File("customerV1.avro");
    final DatumReader<CustomerV2> dr = new SpecificDatumReader<>(CustomerV2.class);
    final DataFileReader<CustomerV2> dfr;
    try {
      dfr = new DataFileReader<CustomerV2>(file,dr);
      while (dfr.hasNext()){
        CustomerV2 cv2 = dfr.next();
        System.out.println("Customer v2" + cv2.toString());
      }
      System.out.println("cool backwards compability!");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
