package com.example.beam;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterShortWordsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String word, OutputReceiver<String> out) {
        if (word.length() >= 3) {
            out.output(word);
        }
    }
}
