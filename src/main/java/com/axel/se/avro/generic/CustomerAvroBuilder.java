package com.axel.se.avro.generic;

import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;


public class CustomerAvroBuilder {

  private static class CustomerAvroFields {
    public static final String FIRST_NAME = "first_name";
    public static final String LAST_NAME = "last_name";
    public static final String AGE = "age";
    public static final String HEIGHT= "height";
    public static final String WEIGHT = "weight";
    public static final String AUTOMATED_EMAIL = "automated_email";

  }


  private final Schema.Parser parser = new Schema.Parser();
  // step 0 define the avro schema
  private final Schema schema = parser.parse(
      "{\n"
          + "     \"type\": \"record\",\n"
          + "     \"namespace\": \"com.axel.se\",\n"
          + "     \"name\": \"Customer\",\n"
          + "     \"doc\": \"My cool avro schema for a customer\",\n"
          + "     \"fields\": [\n"
          + "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First name\" },\n"
          + "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last name\" },\n"
          + "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age of customer\" },\n"
          + "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"height of customer in centimeters\" },\n"
          + "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"weight of customer kilograms\" },\n"
          + "       { \"name\": \"automated_email\", \"type\": \"boolean\",\"default\":true,\n"
          + "         \"doc\": \"true if the user wants marketing emails\" }\n"
          + "     ]  \n"
          + "}"
  );

  public Schema getSchema() {
    return schema;
  }

  private final GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);

  public Record createAxelCustomerAvroRecord(){
    genericRecordBuilder.set(CustomerAvroFields.FIRST_NAME,"Axel");
    genericRecordBuilder.set(CustomerAvroFields.LAST_NAME,"Dahlberg");
    genericRecordBuilder.set(CustomerAvroFields.AGE,31);
    genericRecordBuilder.set(CustomerAvroFields.HEIGHT,182F);
    genericRecordBuilder.set(CustomerAvroFields.WEIGHT,84.0F);
    genericRecordBuilder.set(CustomerAvroFields.AUTOMATED_EMAIL,true);
    return genericRecordBuilder.build();
  }

  public Record createCustomerAvroRecord(String firstName,String lastName,int age,float height,float weight,
      Optional<Boolean> automatedEmail) {
    genericRecordBuilder.set(CustomerAvroFields.FIRST_NAME, firstName);
    genericRecordBuilder.set(CustomerAvroFields.LAST_NAME, lastName);
    genericRecordBuilder.set(CustomerAvroFields.AGE, age);
    genericRecordBuilder.set(CustomerAvroFields.HEIGHT, height);
    genericRecordBuilder.set(CustomerAvroFields.WEIGHT, weight);
    if (automatedEmail.isPresent()) {
      genericRecordBuilder.set(CustomerAvroFields.AUTOMATED_EMAIL, automatedEmail.get());
    }
    return genericRecordBuilder.build();
  }
}
