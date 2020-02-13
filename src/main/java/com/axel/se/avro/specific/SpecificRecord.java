package com.axel.se.avro.specific;

import com.axel.se.Customer;
import com.axel.se.Customer.Builder;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SpecificRecord {

  public static void main(String[] args) {
    // build customer
    Customer.Builder builder = Customer.newBuilder();
    builder.setFirstName("Axel");
    builder.setLastName("Dahlberg");
    builder.setAge(31);
    builder.setAutomatedEmail(false);
    builder.setHeight(182F);
    builder.setWeight(84F);

    Customer axelCustomer = builder.build();
    // write to file
    SpecificDatumWriter<Customer> customerSpecificDatumWriter = new SpecificDatumWriter<>(
        Customer.class);
    File file = new File("customer-specific.avro");
    try (DataFileWriter<Customer> customerDataFileWriter = new DataFileWriter<>(
        customerSpecificDatumWriter)) {
      customerDataFileWriter.create(axelCustomer.getSchema(),file);
      customerDataFileWriter.append(axelCustomer);

    } catch (IOException e) {
      e.printStackTrace();
    }
    // read that file
    final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
    try (DataFileReader<Customer> customerDataFileReader = new DataFileReader<Customer>(file,
        datumReader)) {
      while (customerDataFileReader.hasNext()) {
        Customer nextCustomer = customerDataFileReader.next();
        System.out.printf("Neeext customer!" + nextCustomer);
        System.out.printf("First name: " + nextCustomer.getFirstName());
      }

    } catch (IOException e) {
      e.printStackTrace();
    }


  }

}
