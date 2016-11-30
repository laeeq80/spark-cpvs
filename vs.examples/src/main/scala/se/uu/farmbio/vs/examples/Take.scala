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

object Take extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    sdfPath: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Take") {
      head("Counts number of molecules in conformer file")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
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
      .setAppName("Take")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    val mols = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .getMolecules
      .flatMap { mol => SBVSPipeline.splitSDFmolecules(mol.toString) }
    val parseScoreRDD = mols.map(PosePipeline.parseScore).cache()

    // _.1 contains Range and _.2 contains Number of items in range
    val parseScoreHistogram = parseScoreRDD.histogram(10)
    val idAndMol = mols.map {
      case (mol) =>
        val score = PosePipeline.parseScore(mol)
        VSUtils.assignGroup(mol, score, parseScoreHistogram._1)
    }.cache()
    
    val sample1 = idAndMol.filter { case (id, mol) => (id == 1.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample2 = idAndMol.filter { case (id, mol) => (id == 2.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample3 = idAndMol.filter { case (id, mol) => (id == 3.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample4 = idAndMol.filter { case (id, mol) => (id == 4.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample5 = idAndMol.filter { case (id, mol) => (id == 5.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample6 = idAndMol.filter { case (id, mol) => (id == 6.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample7 = idAndMol.filter { case (id, mol) => (id == 7.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample8 = idAndMol.filter { case (id, mol) => (id == 8.0) }
      .map { case (id, mol) => mol }.sample(false, 0.05, 1234)
    val sample9 = idAndMol.filter { case (id, mol) => (id == 9.0) }
      .map { case (id, mol) => mol }.sample(false, 0.5, 1234)
    val sample10 = idAndMol.filter { case (id, mol) => (id == 10.0) }
      .map { case (id, mol) => mol }.sample(false, 0.5, 1234)
    
    val sample = sample1.union(sample2).union(sample3).union(sample4).union(sample5)
    .union(sample6).union(sample7).union(sample8).union(sample9).union(sample10)
    
    sample.saveAsTextFile(params.sdfPath)
    
    /*
    //Sampling first 8 bins
    var oldSample: RDD[String] = null
    var counter: Double = 0.0
    var newSample: RDD[String] = null
    do {
      counter = counter + 1.0
      newSample = idAndMol.filter { case (id, mol) => (id == counter) }
        .map { case (id, mol) => mol }.sample(false, 0.1, 1234)
      if (oldSample == null)
        oldSample = newSample
      else
        oldSample = oldSample.union(newSample)
    } while (counter <= 8.0)

    oldSample.saveAsTextFile(params.sdfPath)
*/
    /*
    val groupMolByBin = idAndMol.groupBy { case (id, mol) => id }

    val sample = groupMolByBin
      .filter { case (id, idAndMol) => (id < 9.0) }
      .mapValues { idAndMol =>
        VSUtils.takeSample(idAndMol, 1234, ((idAndMol.size) * 0.05).toInt)
          .map { case (id, mol) => mol }.mkString
      }.map { case (id, mol) => mol }
      .filter(_.nonEmpty)
      .flatMap(x => SBVSPipeline.splitSDFmolecules(x)).map(_.trim)

    val sample2 = groupMolByBin
      .filter { case (id, idAndMol) => (id >= 9.0) }
      .mapValues { idAndMol =>
        VSUtils.takeSample(idAndMol, 1234, ((idAndMol.size) * 0.5).toInt)
          .map { case (id, mol) => mol }.mkString
      }.map { case (id, mol) => mol }
      .filter(_.nonEmpty)
      .flatMap(x => SBVSPipeline.splitSDFmolecules(x)).map(_.trim)

    sample.union(sample2).saveAsTextFile(params.sdfPath)
*/
    sc.stop()

  }

}

