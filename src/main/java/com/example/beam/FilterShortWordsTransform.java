package com.example.beam;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FilterShortWordsTransform extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> words) {
        return words.apply("FilterShortWords", ParDo.of(new FilterShortWordsFn()));
    }
}
