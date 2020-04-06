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
package com.google.devrel.dataflow.retail.eventprocessing;

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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ExampleSQLProcess {

  static Boolean REAL = false;

  public static void main(String[] args) {

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    Pipeline p = Pipeline.create();
    PCollection<String> jsonStrings;

    if (REAL) {
      jsonStrings = p.apply(PubsubIO.readStrings().fromTopic("<Your Topic>"));
    } else {
      Instant now = new Instant("2020-02-25T08:00:00.000Z");
      jsonStrings =
          p.apply(
              Create.timestamped(
                  TimestampedValue.of(
                      "{\n"
                          + "    \"connection_start\": 0,\n"
                          + "    \"transaction_start\": 1,\n"
                          + "    \"transaction_end\": 2,\n"
                          + "    \"connection_end\": 4,\n"
                          + "    \"success\": true,\n"
                          + "    \"database_type\": \"cloud_sql\",\n"
                          + "    \"transaction_type\": \"read\",\n"
                          + "    \"load_id\": 123412421\n"
                          + "  }",
                      now),
                  TimestampedValue.of(
                      "  {\n"
                          + "    \"connection_start\": 0,\n"
                          + "    \"transaction_start\": 3,\n"
                          + "    \"transaction_end\": 6,\n"
                          + "    \"connection_end\": 8,\n"
                          + "    \"success\": false,\n"
                          + "    \"database_type\": \"cloud_sql\",\n"
                          + "    \"transaction_type\": \"read\",\n"
                          + "    \"load_id\": 123412422\n"
                          + "  }\n",
                      now.plus(Duration.standardSeconds(2))),
                  TimestampedValue.of(
                      "  {\n"
                          + "    \"connection_start\": 0,\n"
                          + "    \"transaction_start\": 1,\n"
                          + "    \"transaction_end\": 2,\n"
                          + "    \"connection_end\": 4,\n"
                          + "    \"success\": true,\n"
                          + "    \"database_type\": \"cloud_sql\",\n"
                          + "    \"transaction_type\": \"read\",\n"
                          + "    \"load_id\": 123412423\n"
                          + "  }\n",
                      now.plus(Duration.standardSeconds(8)))));
    }
    Schema s = null;
    try {
      s = p.getSchemaRegistry().getSchema(Data.class);
    } catch (NoSuchSchemaException e) {
      e.printStackTrace();
    }
    p.getSchemaRegistry().registerPOJO(Data.class);
    // Convert to Schema Object
    // Should no need to set Coder.... odd
    PCollection<Row> dataCollection =
        jsonStrings.apply(JsonToRow.withSchema(s));

    PCollection<Result> result =
        dataCollection
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(
                SqlTransform.query(
                    "SELECT AVG(latency) AS average_latency, "
                        + "SUM(CAST(success AS FLOAT))/Count(1) as failure_percent ,  query_action"
                        + " FROM (SELECT CAST(connection_end AS FLOAT)-CAST(connection_start AS FLOAT) as latency, "
                        + "(CASE success WHEN true THEN 0 ELSE  1 END) AS success, "
                        + "transaction_type as query_action FROM PCOLLECTION)"
                        + " GROUP BY query_action"))
            .apply(Reify.windows())
            .apply(
                MapElements.into(TypeDescriptor.of(Result.class))
                    .<ValueInSingleWindow<Row>>via(
                        x -> {
                          Result r = new Result();
                          r.query_action = x.getValue().getString("query_action");
                          r.average_latency = x.getValue().getFloat("average_latency");
                          r.failure_percent = x.getValue().getFloat("failure_percent");
                          r.timestamp = x.getTimestamp().getMillis();
                          return r;
                        }));

    result.apply(
        MapElements.<String>into(TypeDescriptors.strings())
            .<Result>via(
                x -> {
                  System.out.println(x);
                  return "";
                }));

    p.run();
  }

  @DefaultSchema(JavaFieldSchema.class)
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
                .setProjectId("bigdatapivot")
                .build();
        FirebaseApp.initializeApp(options);

        db = FirestoreClient.getFirestore();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @ProcessElement
    public void processElement(@Element Result result) {
      DocumentReference docRef = db.collection("users").document("alovelace");
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
        System.out.println("Error on JSON " + ex);
      }
    }
  }
}