package io.kination.vine

import org.apache.spark.sql.connector.read.InputPartition

class VineInputPartition(val rawData: String) extends InputPartition
