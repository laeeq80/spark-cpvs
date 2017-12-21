package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import java.io.PrintWriter


/**
 * @author laeeq
 */

object TakeSample extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    sdfPath: String = null,
    sampleSize: Int = 100)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("TakeSample") {
      head("Creates Sample set as required")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      opt[Int]("sampleSize")
        .text("Size of Sample (default: 100)")
        .action((x, c) => c.copy(sampleSize = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<sdf-Path>")
        .required()
        .text("path to subset SDF file")
        .action((x, c) => c.copy(sdfPath = x))
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
      .readConformerFile(params.conformersFile)
      .getMolecules
      .flatMap { mol => SBVSPipeline.splitSDFmolecules(mol.toString) }
      .takeSample(false, params.sampleSize, 1234)

    val pw = new PrintWriter(params.sdfPath)
    mols.foreach(pw.println(_))
    pw.close

    sc.stop()

  }

}