package com.example.beam;

import org.apache.beam.sdk.transforms.DoFn;

public class TokenizerFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> out) {
        for (String word : line.split("\\W+")) {
            if (!word.isEmpty()) {
                out.output(word);
            }
        }
    }
}
