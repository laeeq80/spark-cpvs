package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline


/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    topPosesPath: String = null,
    dsInitSize: Int = 100,
    numIterations: Int = 50,
    topN: Int = 30)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("DockerWithML") {
      head("DockerWithML makes use of Machine learning for efficient Docking")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      opt[Int]("dsInitSize")
        .text("intial Data to be docked (default: 100)")
        .action((x, c) => c.copy(dsInitSize = x))
      opt[Int]("numIterations")
        .text("number of iternations for the ML model training (default: 100)")
        .action((x, c) => c.copy(numIterations = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
      opt[Int]("topN")
        .text("number of top scoring poses to extract (default: 30).")
        .action((x, c) => c.copy(topN = x))
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
      .setAppName("DockerWithML")
    
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val poses = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures()
      .dockWithML(params.dsInitSize,
        params.numIterations)
    val cachedPoses = poses.getMolecules.cache()
    val res = poses.getTopPoses(params.topN)

    sc.parallelize(res, 1).saveAsTextFile(params.topPosesPath)
    sc.stop()

  }

}

