package org.pentaho.hadoop.shim.common;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class WordCount3 {



    Job job;
    public void submit()
    {
        System.out.println("*********** WordCount3 **************");
        URL resolvedJarUrl = null;
        try
        {
            resolvedJarUrl=new File("/home/raja_d/JOBS/job-exe/JobsKTRs_pentaho-mapreduce-sample_MR2.jar").toURI().toURL();

        }catch(Exception e)
        {
            e.printStackTrace();
        }
        URL[] urls = new URL[] { resolvedJarUrl };
        try ( URLClassLoader loader = new URLClassLoader( urls, this.getClass().getClassLoader() ) ) {
            job = Job.getInstance();
            setJobName("WordCount3");
            Class<?> keyClass = loader.loadClass( "org.apache.hadoop.io.Text" );
            setOutputKeyClass( keyClass );
            Class<?> valueClass = loader.loadClass( "org.apache.hadoop.io.IntWritable" );
            setOutputValueClass( valueClass );
            Class<?> mapper = loader.loadClass( "org.pentaho.hadoop.sample.wordcount.WordCount2$Map" );

            setMapperClass( mapper );
            Class<?> reducer = loader.loadClass( "org.pentaho.hadoop.sample.wordcount.WordCount2$Reduce" );

            setReducerClass( reducer );
            Class<?> inputFormat = loader.loadClass( "org.apache.hadoop.mapreduce.lib.input.TextInputFormat" );
            setInputFormat( inputFormat );
            Class<?> outputFormat = loader.loadClass( "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat" );
            setOutputFormat( outputFormat );

            FileInputFormat.addInputPath(job, new Path("/user/mapr/input"));
            FileOutputFormat.setOutputPath(job, new Path("/user/mapr/ou2"));
            System.out.println("Jar ================================> /home/raja_d/JOBS/job-exe/JobsKTRs_pentaho-mapreduce-sample_MR2.jar");
            job.setJar( "/home/raja_d/JOBS/job-exe/JobsKTRs_pentaho-mapreduce-sample_MR2.jar" );
           // setNumMapTasks( 1 );
           // setNumReduceTasks( 1 );
            job.waitForCompletion(true);
        }catch(Exception e)
        {
            System.out.println("######## inside exception block ##############");
            e.printStackTrace();
        }

        System.out.println("*********** WordCount3 Completed **************");
    }


    public  Job getJob() {
        return job;
    }


    public  void setJobName(String hadoopJobName) {
        System.out.println("########## WordCount2.setJobName ############");
        System.out.println("hadoopJobName ==>"+ hadoopJobName);
        getJob().setJobName( hadoopJobName );
    }

    public  void setOutputKeyClass( Class<?> c ) {
        System.out.println("########## WordCount2.setOutputKeyClass ############");
        System.out.println("c ==>"+ c);
        getJob().setOutputKeyClass( c );
    }
    public  void setOutputValueClass( Class<?> c ) {
        System.out.println("########## WordCount2.setOutputValueClass ############");
        System.out.println("c ==>"+c );
        getJob().setOutputValueClass( c );
    }

    public  void setMapperClass( Class<?> c ) {
        System.out.println("########## WordCount2.setMapperClass ############");
        System.out.println("c ==>"+ c);
        if ( org.apache.hadoop.mapred.Mapper.class.isAssignableFrom( c ) ) {
            System.out.println("inside if block");
            //setUseOldMapApi();
            //getJobConf().setMapperClass( (Class<? extends org.apache.hadoop.mapred.Mapper>) c );
        } else if ( org.apache.hadoop.mapreduce.Mapper.class.isAssignableFrom( c ) ) {
            System.out.println("inside else block");
            getJob().setMapperClass( (Class<? extends org.apache.hadoop.mapreduce.Mapper>) c );
        }
    }

    public  void setReducerClass( Class<?> c ) {
        System.out.println("########## WordCount2.setReducerClass ############");
        System.out.println("c ==>"+ c);
        if ( org.apache.hadoop.mapred.Reducer.class.isAssignableFrom( c ) ) {
            System.out.println("inside if block");
            // setUseOldRedApi();
            // getJobConf().setReducerClass( (Class<? extends org.apache.hadoop.mapred.Reducer>) c );
        } else if ( org.apache.hadoop.mapreduce.Reducer.class.isAssignableFrom( c ) ) {
            System.out.println("inside else block");
            getJob().setReducerClass( (Class<? extends org.apache.hadoop.mapreduce.Reducer>) c );
        }
    }

    public  void setInputFormat( Class<?> inputFormat ) {
        System.out.println("########## WordCount2.setInputFormat ############");
        System.out.println("inputFormat ==>"+ inputFormat);
        if ( org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom( inputFormat ) ) {
            System.out.println("inside if block");
            //setUseOldMapApi();
            //getJobConf().setInputFormat( (Class<? extends org.apache.hadoop.mapred.InputFormat>) inputFormat );
        } else if ( org.apache.hadoop.mapreduce.InputFormat.class.isAssignableFrom( inputFormat ) ) {
            System.out.println("inside else block");
            getJob().setInputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.InputFormat>) inputFormat );
        }
    }

    public  void setOutputFormat( Class<?> outputFormat ) {
        System.out.println("########## WordCount2.setOutputFormat ############");
        System.out.println("outputFormat ==>"+outputFormat );
        if ( org.apache.hadoop.mapred.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
            System.out.println("inside if block");
           /* setUseOldRedApi();
            if ( getJobConf().getNumReduceTasks() == 0 || get( "mapred.partitioner.class" ) != null ) {
                setUseOldMapApi();
            }
            getJobConf().setOutputFormat( (Class<? extends org.apache.hadoop.mapred.OutputFormat>) outputFormat );*/
        } else if ( org.apache.hadoop.mapreduce.OutputFormat.class.isAssignableFrom( outputFormat ) ) {
            System.out.println("inside else block");
            getJob().setOutputFormatClass( (Class<? extends org.apache.hadoop.mapreduce.OutputFormat>) outputFormat );
        }
    }

   /* public JobConf getJobConf() {
        System.out.println("########## WordCount2.getJobConf ############");
        return (JobConf) job.getConfiguration();
    }

    public  void set( String name, String value ) {
        System.out.println("########## WordCount2.setReducerClass ############");
        System.out.println("name ==>"+ name);
        System.out.println("value ==>"+ value);
        getJobConf().set( name, value );
    }*/

   /* public  void setInputPaths( org.pentaho.hadoop.shim.api.internal.fs.Path... paths ) {
        System.out.println("########## WordCount2.setInputPaths ############");
        System.out.println("paths ==>"+ paths.toString());
        if ( paths == null ) {
            return;
        }
        Path[] actualPaths = new Path[ paths.length ];
        for ( int i = 0; i < paths.length; i++ ) {
            actualPaths[ i ] = ShimUtils.asPath( paths[ i ] );
        }
        try {
            System.out.println("actualPaths ==>"+ actualPaths.toString());
            FileInputFormat.setInputPaths( getJob(), actualPaths );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    public  void setOutputPath( org.pentaho.hadoop.shim.api.internal.fs.Path path ) {
        System.out.println("########## WordCount2.setReducerClass ############");
        System.out.println("path ==>"+ path);
        FileOutputFormat.setOutputPath( getJob(), ShimUtils.asPath( path ) );
    }*/

    public  void setJar( String url ) {
        System.out.println("########## WordCount2.setJar ############");
        System.out.println("url ==>"+url );
        getJob().setJar( url );
    }

   /* public  void setNumMapTasks( int n ) {
        System.out.println("########## WordCount2.setReducerClass ############");
        System.out.println("n ==>"+n);
        getJobConf().setNumMapTasks( n );
    }

    public  void setNumReduceTasks( int n ) {
        System.out.println("########## WordCount2.setReducerClass ############");
        System.out.println("n ==>"+ n);
        getJob().setNumReduceTasks( n );
    }*/

    public static void main(final String[] args) {
        WordCount3 c=new WordCount3();
        c.submit();
    }
}
