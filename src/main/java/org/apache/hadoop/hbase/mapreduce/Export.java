/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Date;

import static java.util.Arrays.*;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import static joptsimple.util.DateConverter.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

/**
 * Export an HBase table.
 * Writes content to sequence files up in HDFS.  Use {@link Import} to read it
 * back in again.
 */
public class Export {
  
  final static String NAME = "export";

  private static String tableName = null;
  private static String outputDir = null;
  private static Integer maxVersions = null;
  private static Long startTime = null;
  private static Long endTime = null;
  private static Integer caching = null;
  private static Boolean compress = null;

  /**
   * Mapper.
   */
  static class Exporter
  extends TableMapper<ImmutableBytesWritable, Result> {
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, 
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        context.write(row, value);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Sets up the actual job.
   * 
   * @param conf  The current configuration.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf)
  throws IOException {
    Path outputPath = new Path(outputDir);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Exporter.class);
    // TODO: Allow passing filter and subset of rows/columns.
    Scan scan = new Scan();
    // Optional arguments.
    int versions = maxVersions != null ? maxVersions.intValue() : 1;
    scan.setMaxVersions(versions);
    long startRange = startTime != null ? startTime.longValue() : 0L;
    long endRange = endTime != null ? endTime.longValue() : Long.MAX_VALUE;
    scan.setTimeRange(startRange, endRange);
    if (caching != null) {
      scan.setCaching(caching.intValue());
    }
    Log.info("versions=" + versions + ", starttime=" + startRange +
      ", endtime=" + endRange + ", caching=" + caching + ", compress=" +
      compress);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, Exporter.class,
      null, null, job);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    if (compress != null && compress.booleanValue()) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job,
        org.apache.hadoop.io.compress.GzipCodec.class);
    }
    return job;
  }

  /**
   * Parses the command line arguments.
   *
   * @param args  The command line arguments.
   * @return The parsed options as an OptionSet.
   */
  private static void parseArgs(String[] args) throws IOException {
    OptionParser parser = new OptionParser();
    OptionSpec<String> osTableName = parser.acceptsAll(
      asList("t", "tablename"), "Table name").withRequiredArg();
    OptionSpec<String> osOutputDir = parser.acceptsAll(
      asList("o", "outputdir"), "Output directory").withRequiredArg();
    OptionSpec<Integer> osMaxVersions = parser.acceptsAll(
      asList("n", "versions"), "Maximum versions").
      withRequiredArg().ofType(Integer.class);
    OptionSpec<Long> osStartTime = parser.acceptsAll(
      asList("s", "starttime"), "Start time as long value").
      withRequiredArg().ofType(Long.class);
    OptionSpec<Long> osEndTime = parser.acceptsAll(
      asList("e", "endtime"), "End time as long value").
      withRequiredArg().ofType(Long.class);
    OptionSpec<Date> osStartDate = parser.accepts("startdate",
      "Start date (alternative to --starttime)").
      withRequiredArg().withValuesConvertedBy(datePattern("yyyyMMddHHmm"));
    OptionSpec<Date> osEndDate = parser.accepts("enddate",
      "End date (alternative to --endtime)").
      withRequiredArg().withValuesConvertedBy(datePattern("yyyyMMddHHmm"));
    OptionSpec<Integer> osCaching = parser.acceptsAll(
      asList("c", "caching"), "Number of rows for caching").
      withRequiredArg().ofType(Integer.class);
    OptionSpec osCompress = parser.acceptsAll(asList("z", "compress"),
      "Enable compression of output files");
    OptionSpec osHelp = parser.acceptsAll(asList( "h", "?", "help"),
      "Show this help");
    OptionSet options = parser.parse(args);
    // check if help was invoked or params are missing
    if (!options.has(osTableName) || !options.has(osOutputDir) ||
         options.has(osHelp)) {
      parser.printHelpOn(System.out);
      System.exit(options.has(osHelp) ? 0 : -1);
    }
    // Get everything needed later
    tableName = osTableName.value(options);
    outputDir = osOutputDir.value(options);
    maxVersions = osMaxVersions.value(options);
    startTime = osStartTime.value(options);
    if (options.has(osStartDate)) {
      startTime = osStartDate.value(options).getTime();
    }
    endTime = osEndTime.value(options);
    if (options.has(osEndDate)) {
      endTime = osEndDate.value(options).getTime();
    }
    caching = osCaching.value(options);
    compress = options.has(osCompress);
  }

  /**
   * Main entry point.
   * 
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    parseArgs(otherArgs);
    Job job = createSubmittableJob(conf);
    System.exit(job.waitForCompletion(true)? 0 : 1);
  }
}
