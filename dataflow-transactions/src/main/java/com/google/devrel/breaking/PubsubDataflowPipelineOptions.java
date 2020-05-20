package com.google.devrel.breaking;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface PubsubDataflowPipelineOptions extends DataflowPipelineOptions {

  String getPubsubTopic();

  void setPubsubTopic(String pubsubTopic);
}
