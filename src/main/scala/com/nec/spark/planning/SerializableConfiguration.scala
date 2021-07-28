package com.nec.spark.planning

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

import org.apache.spark.util.Utils

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
}