package com.explore.java_datavec_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.transform.join.Join;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.joda.time.DateTimeZone;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.io.ClassPathResource;

import java.util.Arrays;
import java.util.List;

public class Example2 {

	public static void main (String[] args) throws Exception{
		
		String customerPath = new ClassPathResource("customer.txt").getFile().getPath();
		String transactionPath = new ClassPathResource("transaction.txt").getFile().getPath();
		
		//Datasets
		//Customer Schema
		Schema customerSchema = new Schema.Builder()
				.addColumnLong("customer_id")
				.addColumnsString("customer_name")
				.addColumnCategorical("country", Arrays.asList("Indonesia","USA", "Russia"))
				.build();
		
		//Transaction Schema
		Schema transactionSchema = new Schema.Builder()
				.addColumnLong("customer_id")
				.addColumnTime("purchase_time", DateTimeZone.UTC)
				.addColumnLong("product_id")
				.addColumnLong("qty")
				.addColumnDouble("unit_price")
				.build();
		
		//Spark Configuration
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Datavec Join Example");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
        //Load the data:
        RecordReader rr = new CSVRecordReader();
        JavaRDD<List<Writable>> customerInfo = sc.textFile(customerPath).map(new StringToWritablesFunction(rr));
        JavaRDD<List<Writable>> transactionInfo = sc.textFile(transactionPath).map(new StringToWritablesFunction(rr));
            //Collect data for later printing
        List<List<Writable>> customerInfoList = customerInfo.collect();
        List<List<Writable>> transactionInfoList = transactionInfo.collect();

        //Let's join these two data sets together, by customer ID
        Join join = new Join.Builder(Join.JoinType.Inner)
            .setJoinColumns("customer_id")
            .setSchemas(customerSchema, transactionSchema)
            .build();
		
        JavaRDD<List<Writable>> joinedData = SparkTransformExecutor.executeJoin(join, customerInfo, transactionInfo);
        List<List<Writable>> joinedDataList = joinedData.collect();

        //Stop spark, and wait a second for it to stop logging to console
        sc.stop();
        Thread.sleep(2000);

        //Display the original data
        //Customer data
        System.out.println("\n\n----- Customer Information -----");
        System.out.println("Source file: " + customerPath);
        System.out.println(customerSchema);
        System.out.println("Customer Information Data:");
        System.out.println(customerSchema.getName(0)+"\t"+customerSchema.getName(1)+"\t"+customerSchema.getName(2));
        for(List<Writable> line : customerInfoList){
            System.out.println(line.get(0)+"\t\t"+line.get(1)+"\t"+line.get(2));
        }        
        
        //Transaction data
        System.out.println("\n\n----- Transaction Information -----");
        System.out.println("Source file: " + transactionPath);
        System.out.println(transactionSchema);
        System.out.println("Customer Information Data:");
        System.out.println(transactionSchema.getName(0)+"\t"+transactionSchema.getName(1)+"\t"+transactionSchema.getName(2));
        for(List<Writable> line : transactionInfoList){
            System.out.println(line.get(0)+"\t\t"+line.get(1)+"\t"+line.get(2));
        }
	
        //Display the joined data
        System.out.println("\n\n----- Joined Data -----");
        System.out.println(join.getOutputSchema());
        System.out.println("Joined Data:");
        
        for(List<Writable> line : joinedDataList){
            System.out.println(line);
        } 
	}
}
