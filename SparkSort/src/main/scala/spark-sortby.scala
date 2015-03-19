package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.ByteBuffer

object SparkSort {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: SparkSort <infile> <recordsize> <outfile>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkSort")
    val ctx = new SparkContext(sparkConf)
    val data = ctx.binaryRecords(args(0), args(1).toInt)
    val result = data.sortBy({ s =>
        val b = Arrays.copyOfRange(s, 0, 4);
        val bb = ByteBuffer.wrap(b);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.getInt();
    })
    result.saveAsObjectFile("result.out");

    ctx.stop()
  }
}