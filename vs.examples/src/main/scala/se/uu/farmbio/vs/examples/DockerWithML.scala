package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import se.uu.farmbio.vs.PosePipeline
import se.uu.farmbio.vs.ConformersWithSignsAndScorePipeline

/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    topPosesPath: String = null,
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
    stratified: Boolean = false)

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
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
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
      .getMolecules
      .flatMap { mol => SBVSPipeline.splitSDFmolecules(mol) }

    val posesWithSigns = new ConformersWithSignsAndScorePipeline(poses)
      .dockWithML(params.dsInitSize,
        params.dsIncreSize,
        params.calibrationPercent,
        params.numIterations,
        params.badIn,
        params.goodIn,
        params.singleCycle,
        params.stratified)
    val res = posesWithSigns.getTopPoses(params.topN)

    sc.parallelize(res, 1).saveAsTextFile(params.topPosesPath)

    val mols1 = new SBVSPipeline(sc)
      .readPoseFile(params.firstFile)
      .getMolecules

    val Array1 = mols1.map { mol => PosePipeline.parseScore(mol) }.collect()

    val mols2 = new SBVSPipeline(sc)
      .readPoseFile(params.secondFile)
      .getMolecules

    val Array2 = mols2.map { mol => PosePipeline.parseScore(mol) }.collect()

    var counter: Double = 0.0
    for (i <- 0 to Array1.length - 1)
      for (j <- 0 to Array2.length - 1)
        if (Array1(i) == Array2(j))
          counter = counter + 1
    logInfo("JOB_INFO: Bad bins ranges from 0-" + params.badIn +
      " and good bins ranges from " + params.goodIn + "-10")
    logInfo("JOB_INFO: Number of molecules matched are " + counter)
    logInfo("JOB_INFO: Percentage of same results is " + (counter / params.topN) * 100)

    sc.stop()

  }

}

