package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import se.uu.farmbio.vs.PosePipeline
import java.io.PrintWriter

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution

/**
 * @author laeeq
 */

object Compare extends Logging {

  case class Arglist(
    master: String = null,
    firstFile: String = null,
    secondFile: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Compare") {
      head("Counts number of molecules in conformer file")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<first-file>")
        .required()
        .text("path to input first file")
        .action((x, c) => c.copy(firstFile = x))
      arg[String]("<second-file>")
        .required()
        .text("path to input second file")
        .action((x, c) => c.copy(secondFile = x))
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

    val mols1 = new SBVSPipeline(sc)
      .readPoseFile(params.firstFile, OEDockMethod.Chemgauss4)
      .getMolecules
      .flatMap {  mol => SBVSPipeline.splitSDFmolecules(mol.toString) }
      
    val scores1 = mols1.map { mol => PosePipeline.parseId(mol) }
    
    val Array1 = scores1.collect()
    
    val mols2 = new SBVSPipeline(sc)
      .readPoseFile(params.secondFile, OEDockMethod.Chemgauss4)
      .getMolecules
      .flatMap {  mol => SBVSPipeline.splitSDFmolecules(mol.toString) }
   
    val scores2 = mols2.map { mol => PosePipeline.parseId(mol) }
    
    val Array2 = scores2.collect()
    
    var counter : Double = 0.0
    for (i <- 0 to Array1.length - 1)
      for (j <- 0 to Array2.length - 1)
        if (Array1(i) == Array2(j))
          counter = counter + 1
   println(s"Number of molecules matched are " + counter)
   
   println (s"Percentage of same results is " + (counter/30)*100  ) 

    sc.stop()

  }

}
