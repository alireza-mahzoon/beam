package com.example.beam;

import groovy.lang.Tuple2;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class WordCount
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
//        Pipeline pipeline = Pipeline.create();
//
//        // Create a PCollection of words
//        PCollection<String> words = pipeline.apply(Create.of(Arrays.asList(
//                "hello",
//                "world",
//                "hello",
//                "apache",
//                "beam",
//                "hello",
//                "hello"
//        )));
//
//        // Apply transformations to count words
//        PCollection<String> wordCounts = words.apply(Count.perElement())
//                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//                    @Override
//                    public String apply(KV<String, Long> input) {
//                        System.out.println("Word: " + input.getKey() + ", Count: " + input.getValue());
//                        return "Word: " + input.getKey() + ", Count: " + input.getValue();
//                    }
//                }));
//
//
//        // Run the pipeline
//        pipeline.run();


        // Create a Beam pipeline
        Pipeline pipeline = Pipeline.create();

        // Read input text file
        pipeline.apply("ReadLines", TextIO.read().from("/Users/alirezamahzoon/Documents/repo/GCP/beam/src/main/resources/input.txt"))

                // Split lines into words
                .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> java.util.Arrays.asList(line.split("\\W+"))))

                // Count occurrences of each word
                .apply("CountWords", Count.perElement())

                // Format the results
                .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) ->
                                wordCount.getKey() + ": " + wordCount.getValue()))

                // Write the results to an output file
                .apply("WriteResults", TextIO.write().to("output.txt").withSuffix(".txt").withNumShards(1));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

}

