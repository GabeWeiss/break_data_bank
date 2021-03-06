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
import com.google.gson.reflect.TypeToken;
import java.awt.*;
import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BreakingDataTransactions {

    // When true, this pulls from the specified Pub/Sub topic
  static Boolean REAL = true;
    // when set to true the job gets deployed to Cloud Dataflow
  static Boolean DEPLOY = true;

  private static final Logger LOG = LoggerFactory.getLogger(FireStoreOutput.class);

  public static void main(String[] args) {

    PipelineOptionsFactory.register(PubsubDataflowPipelineOptions.class);
    PubsubDataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create().as(PubsubDataflowPipelineOptions.class);

    if (DEPLOY) {
        options.setRunner(DataflowRunner.class);
        options.setTempLocation(options.getGcpTempLocation());
        options.setRegion(options.getRegion());
    }

    Pipeline p = Pipeline.create(options);

    PCollection<String> jsonStrings;

    if (REAL) {
      jsonStrings = p.apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic()));
    } else {
      Instant now = new Instant();
      jsonStrings =
          p.apply(
              Create.timestamped(
                  TimestampedValue.of(
                      "[ "
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.809202855, "
                          + "\"connection_end\": 1219110.814320733, "
                          + "\"transaction_start\": 1219110.80930547, "
                          + "\"transaction_end\": 1219110.810471273}, "

                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.80192983, "
                          + "\"connection_end\": 1219110.818258031, "
                          + "\"transaction_start\": 1219110.802201267, "
                          + "\"transaction_end\": 1219110.804397151}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.80463735, "
                          + "\"connection_end\": 1219110.816569818, "
                          + "\"transaction_start\": 1219110.804718464, "
                          + "\"transaction_end\": 1219110.806886593}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.812853771, "
                          + "\"connection_end\": 1219110.820495596, "
                          + "\"transaction_start\": 1219110.812978694, "
                          + "\"transaction_end\": 1219110.816987723}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.818118327, "
                          + "\"connection_end\": 1219110.822654367, "
                          + "\"transaction_start\": 1219110.81897456, "
                          + "\"transaction_end\": 1219110.820711101}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.820620873, "
                          + "\"connection_end\": 1219110.823521713, "
                          + "\"transaction_start\": 1219110.820799422, "
                          + "\"transaction_end\": 1219110.822552412}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.824808039, "
                          + "\"connection_end\": 1219110.825840374, "
                          + "\"transaction_start\": 1219110.824886913, "
                          + "\"transaction_end\": 1219110.825233627}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.829361968, "
                          + "\"connection_end\": 1219110.830783106, "
                          + "\"transaction_start\": 1219110.829457245, "
                          + "\"transaction_end\": 1219110.829868906}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.833400321, "
                          + "\"connection_end\": 1219110.83450169, "
                          + "\"transaction_start\": 1219110.833480902, "
                          + "\"transaction_end\": 1219110.833838753}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.836986726, "
                          + "\"connection_end\": 1219110.838121447, "
                          + "\"transaction_start\": 1219110.837086874, "
                          + "\"transaction_end\": 1219110.837416985}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.841782659, "
                          + "\"connection_end\": 1219110.843185472, "
                          + "\"transaction_start\": 1219110.841866135, "
                          + "\"transaction_end\": 1219110.842263966}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.845508843, "
                          + "\"connection_end\": 1219110.846875106, "
                          + "\"transaction_start\": 1219110.845590623, "
                          + "\"transaction_end\": 1219110.845949925}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.849355644, "
                          + "\"connection_end\": 1219110.850616399, "
                          + "\"transaction_start\": 1219110.849459986, "
                          + "\"transaction_end\": 1219110.849797437}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.853090156, "
                          + "\"connection_end\": 1219110.85418448, "
                          + "\"transaction_start\": 1219110.85317578, "
                          + "\"transaction_end\": 1219110.85363494}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.856722351, "
                          + "\"connection_end\": 1219110.857815271, "
                          + "\"transaction_start\": 1219110.856807875, "
                          + "\"transaction_end\": 1219110.857228336}"
                      + "]",
                      now),
                  TimestampedValue.of(
                      "[ "
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.845508843, "
                          + "\"connection_end\": 1219110.846875106, "
                          + "\"transaction_start\": 1219110.845590623, "
                          + "\"transaction_end\": 1219110.845949925}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.849355644, "
                          + "\"connection_end\": 1219110.850616399, "
                          + "\"transaction_start\": 1219110.849459986, "
                          + "\"transaction_end\": 1219110.849797437}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.853090156, "
                          + "\"connection_end\": 1219110.85418448, "
                          + "\"transaction_start\": 1219110.85317578, "
                          + "\"transaction_end\": 1219110.85363494}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.856722351, "
                          + "\"connection_end\": 1219110.857815271, "
                          + "\"transaction_start\": 1219110.856807875, "
                          + "\"transaction_end\": 1219110.857228336}"
                        + "]",
                      now.plus(Duration.standardSeconds(6))),
                  TimestampedValue.of(
                      "[ "
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.845508843, "
                          + "\"connection_end\": 1219110.846875106, "
                          + "\"transaction_start\": 1219110.845590623, "
                          + "\"transaction_end\": 1219110.845949925}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.849355644, "
                          + "\"connection_end\": 1219110.850616399, "
                          + "\"transaction_start\": 1219110.849459986, "
                          + "\"transaction_end\": 1219110.849797437}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"read\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.853090156, "
                          + "\"connection_end\": 1219110.85418448, "
                          + "\"transaction_start\": 1219110.85317578, "
                          + "\"transaction_end\": 1219110.85363494}, "
                          
                          + "{\"workload_id\": \"83f02857-e970-4b5a-8418-64a8cc4308a7\", "
                          + "\"job_id\": \"cPBcIkgQssFELSUJCCmJ\", "
                          + "\"operation\": \"write\", "
                          + "\"success\": true, "
                          + "\"connection_start\": 1219110.856722351, "
                          + "\"connection_end\": 1219110.857815271, "
                          + "\"transaction_start\": 1219110.856807875, "
                          + "\"transaction_end\": 1219110.857228336}"
                        + "]",
                      now.plus(Duration.standardSeconds(12)))));
    }

    // Convert to Schema Object
    // Should no need to set Coder.... odd
    PCollection<Data> dataCollection =
        jsonStrings
            .apply(ParDo.of(JSONToPOJO.create(Data.class)))
            .setCoder(AvroCoder.of(Data.class));

    PCollection<Result> result =
        dataCollection
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
            .apply(WithKeys.of(x -> x.operation + "-" + x.job_id))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(Data.class)))
            .apply(Combine.<String, Data, Result>perKey(new DataAnalysis()))
            .apply(Reify.windowsInValue())
            .apply(MapElements.into(TypeDescriptor.of(Result.class))
                    .<KV<String, ValueInSingleWindow<Result>>>via(
                        x -> {
                          Result r = new Result();
                          String key = x.getKey();
                          r.query_action = key.substring(0, key.indexOf("-"));
                          r.job_id = key.substring(key.indexOf("-") + 1);
                          r.average_latency = x.getValue().getValue().average_latency;
                          r.failure_percent = x.getValue().getValue().failure_percent;
                          r.timestamp = x.getValue().getTimestamp().getMillis();
                          return r;
                        }));

          // this node will (hopefully) actually write out to Firestore
        result.apply(ParDo.of(new FireStoreOutput()));

    p.run();
  }

  public static class DataAnalysis extends CombineFn<Data, ResultAggregate, Result> {

    @Override
    public ResultAggregate createAccumulator() {
      return new ResultAggregate();
    }

    @Override
    public ResultAggregate addInput(ResultAggregate mutableAccumulator, Data input) {
      mutableAccumulator.totalCount += 1;
      if (!input.success) {
        mutableAccumulator.fail++;
      }
      else {
        mutableAccumulator.latencySum += (input.transaction_end - input.transaction_start);
        mutableAccumulator.latencyCount++;
      }
      return mutableAccumulator;
    }

    @Override
    public ResultAggregate mergeAccumulators(Iterable<ResultAggregate> accumulators) {
      ResultAggregate resultAggregate = createAccumulator();
      for (ResultAggregate r : accumulators) {
        resultAggregate.totalCount += r.totalCount;
        resultAggregate.latencyCount += r.latencyCount;
        resultAggregate.fail += r.fail;
        resultAggregate.latencySum += r.latencySum;
      }
      return resultAggregate;
    }

    @Override
    public Result extractOutput(ResultAggregate accumulator) {
      Result result = new Result();
      result.average_latency = accumulator.latencySum / (double) accumulator.latencyCount;
      result.failure_percent = ((float) accumulator.fail / (float) accumulator.totalCount) * 100;
      return result;
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class Data {
    public float connection_start;  // when connection started
    public double transaction_start; // when transaction started
    public double transaction_end;   // when transaction ended
    public float connection_end;    // when connection ended
    public boolean success;         // whether it succeeded or not
    public String operation;        // read/write
    public String workload_id;      // ignored, this is the load-gen-script's UUID
    public String job_id;           // holds the Firestore doc ID for db run (type, read/write pattern, intensity)
  }

  @DefaultCoder(AvroCoder.class)
  public static class ResultAggregate {
    public int fail;
    public double latencySum;
    public int latencyCount;
    public int totalCount;
  }

  @DefaultCoder(AvroCoder.class)
  public static class Result {
    public float failure_percent;
    public double average_latency;
    @Nullable public String query_action;
    @Nullable public String job_id;
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

  public static class FireStoreOutput extends DoFn<Result, String> {

    static Firestore db;

    public static synchronized Firestore getDB() {
      if (db == null) {
          // Initialize our Firestore instance
        try {
            // this will grab the service account assigned to the
            // Dataflow pipeline from commandline options. It's passed
            // in from the Maven deployment command as --serviceAccount=<account>
          GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
          //LOG.info("Credentials: " + credentials);

            // Need the project ID in order to initialize Firestore
            // Easiest way is to fetch from the GCE instance's metadata serve
          URL url = new URL("http://metadata.google.internal/computeMetadata/v1/project/project-id");
          HttpURLConnection con = (HttpURLConnection) url.openConnection();
          con.setRequestProperty("Metadata-Flavor", "Google");
          con.setRequestMethod("GET");
          int status = con.getResponseCode();
          BufferedReader in = new BufferedReader(
            new InputStreamReader(con.getInputStream()));
          String inputLine;
          StringBuffer content = new StringBuffer();
          while ((inputLine = in.readLine()) != null) {
              content.append(inputLine);
          }
          in.close();
          con.disconnect();

          String projectId = content.toString();

          FirebaseOptions firebaseOptions =
              new FirebaseOptions.Builder()
                  .setCredentials(credentials)
                  .setProjectId(projectId)
                  .build();
          FirebaseApp firebaseApp = FirebaseApp.initializeApp(firebaseOptions);
        } catch (Exception ex) {
          LOG.error("Error in initializing Firestore:" + ex);
        }
        db = FirestoreClient.getFirestore();
      }
      return db;
    }

    @ProcessElement
    public void processElement(@Element Result result) {
      DocumentReference docRef = getDB().collection("events")
                                   .document("next2020")
                                   .collection("transactions")
                                   .document(result.job_id)
                                   .collection("transactions")
                                   .document();
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
        Type type = new TypeToken<List<T>>() {}.getType();

        List<T> array = gson.fromJson(input,type);

        for(T e : array) {
          o.output(gson.fromJson(e.toString(),clazz));
        }

      } catch (Exception ex) {
        LOG.error("Error in processing:" + ex);
      }
    }
  }
}

