package com.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCount {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from("src/main/resources/input.txt"));

        PCollection<String> words = lines.apply(new ExtractWordsTransform());
        PCollection<String> filteredWords = words.apply(new FilterShortWordsTransform());
        PCollection<KV<String, Long>> wordCounts = filteredWords.apply("CountWords", Count.perElement());

        wordCounts
                .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply("WriteResults", TextIO.write().to("output").withSuffix(".txt").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
