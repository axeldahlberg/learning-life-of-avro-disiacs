package com.axel.se.avro.reflection;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class ReflectExample {

  public static void main(String[] args) {
    // dont like it
    String x = null;
    ReflectCustomer rc = new ReflectCustomer("f","l",null);
    // fetch schema from the class
    Schema schema = ReflectData.get().getSchema(ReflectCustomer.class);
    System.out.println(schema.toString(true));
    // create a file .... make it possible to send over kafka.
    File file = new File("customer-reflected.avro");
    try {
      System.out.println("writing");
      DatumWriter<ReflectCustomer> writer = new ReflectDatumWriter<>(ReflectCustomer.class);
      DataFileWriter<ReflectCustomer> out = new DataFileWriter<>(writer)
          .setCodec(CodecFactory.deflateCodec(9))
          .create(schema,file);
      out.append(new ReflectCustomer("Axel","Dahlberg","Fat Ack"));
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // baby read that file!
    try {
      ReflectDatumReader<ReflectCustomer> reader = new ReflectDatumReader<>(
          ReflectCustomer.class);
      DataFileReader<ReflectCustomer> in = new DataFileReader<>(file,reader);
      for ( ReflectCustomer r : in) {
        System.out.println(r.getFirstName());
      }
      // close the reader input
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

class ReflectCustomer {
  private String firstName;
  private String lastName;
  @Nullable private String nickName;

  public ReflectCustomer(){}

  public ReflectCustomer(String firstName, String lastName, String nickName) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.nickName = nickName;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getNickName() {
    return nickName;
  }

  public void setNickName(String nickName) {
    this.nickName = nickName;
  }
}
