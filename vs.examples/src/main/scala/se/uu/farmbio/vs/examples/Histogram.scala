package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import se.uu.farmbio.vs.PosePipeline
import java.io.PrintWriter

import se.uu.farmbio.vs.VSUtils

/**
 * @author laeeq
 */

object Histogram extends Logging {

  case class Arglist(
    master: String = null,
    poseFile: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Histogram") {
      head("Creates Histogram from dataset")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<pose-file>")
        .required()
        .text("path to input pose file")
        .action((x, c) => c.copy(poseFile = x))
      
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {

    //Init Spark
    val conf = new SparkConf()
      .setAppName("SampleSet")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val mols = new SBVSPipeline(sc)
      .readPoseFile(params.poseFile)
      .getMolecules
    val parseScoreRDD = mols.map(PosePipeline.parseScore).cache()

    // _.1 contains Range and _.2 contains Number of items in range
    val parseScoreHistogram = parseScoreRDD.histogram(10)
    
    println("The Range is ")
    parseScoreHistogram._1.foreach(println(_))
    println("The number of items in each range is")
    parseScoreHistogram._2.foreach(println(_))   
    sc.stop()

  }

}

