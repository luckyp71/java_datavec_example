package com.explore.java_datavec_examples;

import java.io.File;
import java.util.Arrays;

import org.datavec.api.transform.schema.Schema;
import org.datavec.api.util.ClassPathResource;


public class Example1 {

	public static void main (String[] args) {
		Schema inputDataSchema = new Schema.Builder()
				//Define a single column
				.addColumnString("Name")
				.addColumnInteger("Age")
				.addColumnCategorical("CountryCode", Arrays.asList("USA", "INA", "AUS"))
				.build();
		
		//Print out the schema
		System.out.println("Input data schema details:");
		System.out.println(inputDataSchema);
		
		System.out.println("\n\nOther information obtainable from schema:");
		System.out.println("Number of columns: "+inputDataSchema.numColumns());
		System.out.println("Column names: "+inputDataSchema.getColumnNames());
		System.out.println("Column types: "+inputDataSchema.getColumnTypes());
		
		ClassPathResource resources = new ClassPathResource("");
		File directory = new File("resources");
		String test = directory.getAbsolutePath();
		System.out.println(test);
		
	}	
	
}
