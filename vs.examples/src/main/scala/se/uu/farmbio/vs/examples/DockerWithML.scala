package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution

/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    topPosesPath: String = null,
    receptorFile: String = null,
    oeLicensePath: String = null,
    firstFile: String = null,
    secondFile: String = null,
    dsInitSize: Int = 100,
    dsIncreSize: Int = 50,
    calibrationPercent: Double = 0.3,
    numIterations: Int = 50,
    topN: Int = 30,
    badIn: Int = 1,
    goodIn: Int = 4,
    singleCycle: Boolean = false,
    stratified: Boolean = false,
    confidence: Double = 0.2
  )

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("DockerWithML") {
      head("DockerWithML makes use of Machine learning for efficient Docking")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<receptor-file>")
        .required()
        .text("path to input OEB receptor file")
        .action((x, c) => c.copy(receptorFile = x))
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
      opt[String]("oeLicensePath")
        .text("path to OEChem License")
        .action((x, c) => c.copy(oeLicensePath = x))
      arg[String]("<first-file>")
        .required()
        .text("path to input file with top N mols")
        .action((x, c) => c.copy(firstFile = x))
      arg[String]("<second-file>")
        .required()
        .text("path to input file that you want to check for accuracy")
        .action((x, c) => c.copy(secondFile = x))
      opt[Int]("dsInitSize")
        .text("initial Data Size to be docked (default: 100)")
        .action((x, c) => c.copy(dsInitSize = x))
      opt[Int]("dsIncreSize")
        .text("incremental Data Size to be docked (default: 50)")
        .action((x, c) => c.copy(dsIncreSize = x))
      opt[Double]("calibrationPercent")
        .text("calibration Percent from training set (default: 0.3)")
        .action((x, c) => c.copy(calibrationPercent = x))
      opt[Int]("numIterations")
        .text("number of iternations for the ML model training (default: 100)")
        .action((x, c) => c.copy(numIterations = x))
      opt[Int]("badIn")
        .text("UpperBound of bad bins")
        .action((x, c) => c.copy(badIn = x))
      opt[Int]("goodIn")
        .text("LowerBound of good bins")
        .action((x, c) => c.copy(goodIn = x))
      opt[Int]("topN")
        .text("number of top scoring poses to extract (default: 30).")
        .action((x, c) => c.copy(topN = x))
      opt[Unit]("singleCycle")
        .text("if set the model training will be done only once (for testing purposes)")
        .action((_, c) => c.copy(singleCycle = true))
      opt[Unit]("stratified")
        .text("if set, stratified sampling is performed for calibrationSplit")
        .action((_, c) => c.copy(stratified = true))
      opt[Double]("confidence")
        .text("confidence for conformal prediction (default: 1 - 0.2)")
        .action((x, c) => c.copy(confidence = x))
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
    if (params.oeLicensePath != null) {
      conf.setExecutorEnv("OE_LICENSE", params.oeLicensePath)
    }
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val poses = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures()
      .dockWithML(params.receptorFile,
        OEDockMethod.Chemgauss4,
        OESearchResolution.Standard,
        params.dsInitSize,
        params.dsIncreSize,
        params.calibrationPercent,
        params.numIterations,
        params.badIn,
        params.goodIn,
        params.singleCycle,
        params.stratified,
        params.confidence)
    val cachedPoses = poses.getMolecules.cache()
    val res = poses.getTopPoses(params.topN)

    sc.parallelize(res, 1).saveAsTextFile(params.topPosesPath)
    sc.stop()

  }

}

