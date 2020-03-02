/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.devrel.breaking;

import com.alibaba.fastjson.JSONObject;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.awt.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BreakingDataTransactions {

    // When true, this pulls from the specified Pub/Sub topic
  static Boolean REAL = true;
    // when set to true the job gets deployed to Cloud Dataflow
  static Boolean DEPLOY = false;

  public static void main(String[] args) {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    Pipeline p;
    if (DEPLOY)
      p = Pipeline.create(options);
    else
      p = Pipeline.create();

    options.setProject("gweiss-simple-path");
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://gweiss-breaking-test/tmp");

    PCollection<String> jsonStrings;

    if (REAL) {
      String pubsubTopic = "projects/gweiss-simple-path/topics/breaking-test";
      jsonStrings = p.apply(PubsubIO.readStrings().fromTopic(pubsubTopic));
    } else {
      Instant now = new Instant();
      jsonStrings =
          p.apply(
              Create.timestamped(
                  TimestampedValue.of(
                      "{ "
                          + "\"connection_start\": 0, "
                          + "\"transaction_start\": 1, "
                          + "\"transaction_end\": 2, "
                          + "\"connection_end\": 4, "
                          + "\"success\": true, "
                          + "\"database_type\": \"cloud_sql\", "
                          + "\"transaction_type\": \"read\", "
                          + "\"load_id\": 123412421 "
                          + "}",
                      now),
                  TimestampedValue.of(
                      "{ "
                          + "\"connection_start\": 0, "
                          + "\"transaction_start\": 3, "
                          + "\"transaction_end\": 6, "
                          + "\"connection_end\": 8, "
                          + "\"success\": false, "
                          + "\"database_type\": \"cloud_sql\", "
                          + "\"transaction_type\": \"read\", "
                          + "\"load_id\": 123412422 "
                          + "}",
                      now.plus(Duration.standardSeconds(2))),
                  TimestampedValue.of(
                      "{ "
                          + "\"connection_start\": 0, "
                          + "\"transaction_start\": 1, "
                          + "\"transaction_end\": 2, "
                          + "\"connection_end\": 4, "
                          + "\"success\": true, "
                          + "\"database_type\": \"cloud_sql\", "
                          + "\"transaction_type\": \"read\", "
                          + "\"load_id\": 123412423 "
                          + "}",
                      now.plus(Duration.standardSeconds(8)))));
    }

    // Convert to Schema Object
    // Should no need to set Coder.... odd
    PCollection<Data> dataCollection =
        jsonStrings
            .apply(ParDo.of(JSONToPOJO.create(Data.class)))
            .setCoder(AvroCoder.of(Data.class));

    try {
    PCollection<Result> result =
        dataCollection
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(WithKeys.of(x -> x.transaction_type))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Data.class)))
            .apply(Combine.<String, Data, Result>perKey(new DataAnalysis()))
            .apply(Reify.windowsInValue())
            .apply(
                MapElements.into(TypeDescriptor.of(Result.class))
                    .<KV<String, ValueInSingleWindow<Result>>>via(
                        x -> {
                          Result r = new Result();
                          r.query_action = x.getKey();
                          r.average_latency = x.getValue().getValue().average_latency;
                          r.failure_percent = x.getValue().getValue().failure_percent;
                          r.timestamp = x.getValue().getTimestamp().getMillis();
                          return r;
                        }));

    result.apply(
        MapElements.<String>into(TypeDescriptors.strings())
            .<Result>via(
                x -> {
                  System.out.println(x);
                  System.out.println("HOLY SHIT");
                  return "";
                }));
      } catch(Exception ex) {
        System.out.println("An exception occurred");
      }

    p.run();
  }

  public static class DataAnalysis extends CombineFn<Data, ResultAggregate, Result> {

    @Override
    public ResultAggregate createAccumulator() {
      System.out.println("Result Aggregate created");
      return new ResultAggregate();
    }

    @Override
    public ResultAggregate addInput(ResultAggregate mutableAccumulator, Data input) {
      mutableAccumulator.count += 1;
      mutableAccumulator.fail += (input.success) ? 0 : 1;
      mutableAccumulator.latencySum += input.connection_end - input.connection_start;
      System.out.println("addingInput");
      return mutableAccumulator;
    }

    @Override
    public ResultAggregate mergeAccumulators(Iterable<ResultAggregate> accumulators) {
      System.out.println("mergeAccumulators called");
      ResultAggregate resultAggregate = createAccumulator();
      for (ResultAggregate r : accumulators) {
        resultAggregate.count += r.count;
        resultAggregate.fail += r.fail;
        resultAggregate.latencySum += r.latencySum;
      }
      return resultAggregate;
    }

    @Override
    public Result extractOutput(ResultAggregate accumulator) {
      Result result = new Result();
      result.average_latency = accumulator.latencySum / accumulator.count;
      result.failure_percent = ((float) accumulator.fail / (float) accumulator.count) * 100;
      System.out.println("I'm extracting some output");
      return result;
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class Data {
    public int connection_start;
    public int transaction_start;
    public int transaction_end;
    public int connection_end;
    public boolean success;
    public String database_type;
    public String transaction_type;
    public int load_id;
  }

  @DefaultCoder(AvroCoder.class)
  public static class ResultAggregate {
    public int fail;
    public int latencySum;
    public int count;
  }

  @DefaultCoder(AvroCoder.class)
  public static class Result {
    public float failure_percent;
    public float average_latency;
    @Nullable public String query_action;
    public long timestamp;

    @Override
    public String toString() {
      return String.format(
          "%s , %s, %s , %s",
          this.average_latency,
          this.failure_percent,
          this.query_action,
          new Instant(this.timestamp));
    }
  }

  public static class FireStoreOutput extends DoFn<Result, PDone> {

    Firestore db;

    @Setup
    public void setup() {
      GoogleCredentials credentials = null;
      try {
        credentials = GoogleCredentials.getApplicationDefault();

        FirebaseOptions options =
            new FirebaseOptions.Builder()
                .setCredentials(credentials)
                .setProjectId("gweiss-simple-path")
                .build();
        FirebaseApp.initializeApp(options);

        db = FirestoreClient.getFirestore();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @ProcessElement
    public void processElement(@Element Result result) {
    
      DocumentReference docRef = db.collection("events").document("next2020").collection("transactions").document();
      // Add document data  with id "alovelace" using a hashmap
      Map<String, Object> data = new HashMap<>();
      data.put("failure_percent", result.failure_percent);
      data.put("average_latency", result.average_latency);
      data.put("query_action", result.query_action);
      data.put("timestamp", result.timestamp);

      // asynchronously write data
      ApiFuture<WriteResult> writeResult = docRef.set(data);
      try {
        writeResult.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
      ;
    }
  }

  private static class JSONToPOJO<T> extends DoFn<String, T> {

    Class<T> clazz;
    Gson gson;

    public static <T> JSONToPOJO<T> create(Class<T> clazz) {
      return new JSONToPOJO<>(clazz);
    }

    public JSONToPOJO(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Setup
    public void setup() {
      gson = new GsonBuilder().serializeNulls().create();
    }

    @ProcessElement
    public void process(@Element String input, @Timestamp Instant timestamp, OutputReceiver<T> o) {
      try {
        o.output(gson.fromJson(input, clazz));
      } catch (Exception ex) {
        System.out.println(ex);
      }
      System.out.println(input);
    }
  }
}
