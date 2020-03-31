package com.google.devrel.breaking;

public class GlobalVars {
    // static environment variable keys
  static String ENVVAR_PROJECT    = "BREAKING_PROJECT";
  static String ENVVAR_PUBSUB     = "BREAKING_PUBSUB";
  static String ENVVAR_GCS_BUCKET = "BREAKING_DATAFLOW_BUCKET";

  static String projectId = System.getenv(ENVVAR_PROJECT);
  static String pubsubTopic = "projects/" + projectId + "/topics/" + System.getenv(ENVVAR_PUBSUB);
  static String gcsBucket = System.getenv(ENVVAR_GCS_BUCKET) + "/tmp";
}
