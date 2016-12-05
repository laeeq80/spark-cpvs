package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import se.uu.farmbio.vs.ConformersWithSignsAndScorePipeline
import java.io.PrintWriter


/**
 * @author laeeq
 */

object ConvertToLibSVM extends Logging {

  case class Arglist(
    master: String = null,
    poseFile: String = null,
    libSvmFile: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("ConvertToLibSVM") {
      head("Converts Pose File with signatures to LibSVM format")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<Pose-file>")
        .required()
        .text("path to input Pose file")
        .action((x, c) => c.copy(poseFile = x))
      arg[String]("<libSVMFile-Path>")
        .required()
        .text("output path for libSVM file")
        .action((x, c) => c.copy(libSvmFile = x))
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
      .setAppName("TakeSample")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val mols = new SBVSPipeline(sc)
      .readPoseFile(params.poseFile)
      .getMolecules
      
    val libSVMMols = mols.flatMap { mol => ConformersWithSignsAndScorePipeline.getLPRDD_Score(mol) }
    val singlePartition = libSVMMols.repartition(1)
    MLUtils.saveAsLibSVMFile(singlePartition, params.libSvmFile)
    sc.stop()

  }

}
