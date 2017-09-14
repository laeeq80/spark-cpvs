package se.uu.farmbio.vs

import se.uu.farmbio.sg.SGUtils

import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Paths

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.Logging

import org.openscience.cdk.io.SDFWriter
import org.openscience.cdk.interfaces.IAtomContainer

trait ConformerTransforms {
  def dock(receptorPath: String, dockTimePerMol: Boolean = false): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms
  def generateSignatures(): SBVSPipeline with ConformersWithSignsTransforms 
}

object ConformerPipeline extends Logging {
  val VINA_DOCKING_URL = "http://pele.farmbio.uu.se/cpvs-vina/multivina.sh"
  val VINA_CONF_URL = "http://pele.farmbio.uu.se/cpvs-vina/conf.txt"
  val VINA_HOME = "http://pele.farmbio.uu.se/cpvs-vina/"
  //The Spark built-in pipe splits molecules line by line, we need a custom one
  private[vs] def pipeString(str: String, command: List[String]) = {

    //Start executable
    val pb = new ProcessBuilder(command.asJava)
    val proc = pb.start
    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start
    // Start a thread to feed the process input 
    new Thread("stdin writer") {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)
        out.println(str)
        out.close()
      }
    }.start
    //Return results as a single string
    Source.fromInputStream(proc.getInputStream).mkString

  }

  private[vs] def getDockingRDD(receptorPath: String, dockTimePerMol: Boolean, sc: SparkContext, rdd: RDD[String]) = {
    //Use local sh file if VINA_DOCKING is set
    val vinaDockingPath = if (System.getenv("VINA_DOCKING") != null) {
      logInfo("JOB_INFO: using local multivana: " + System.getenv("VINA_DOCKING"))
      System.getenv("VINA_DOCKING")
    } else {
      logInfo("JOB_INFO: using remote multivina: " + VINA_DOCKING_URL)
      VINA_DOCKING_URL
    }
    
    //Use local vina conf.txt file if VINA_CONF is set
    val vinaConfPath = if (System.getenv("VINA_CONF") != null) {
      logInfo("JOB_INFO: using local vina conf: " + System.getenv("VINA_CONF"))
      System.getenv("VINA_CONF")
    } else {
      logInfo("JOB_INFO: using remote vina conf: " + VINA_CONF_URL)
      VINA_CONF_URL
    }
    
    sc.addFile(vinaConfPath)
    sc.addFile(vinaDockingPath)
    sc.addFile(receptorPath)
    val receptorFileName = Paths.get(receptorPath).getFileName.toString
    val dockingstdFileName = Paths.get(vinaDockingPath).getFileName.toString
    val confFileName = Paths.get(vinaConfPath).getFileName.toString
    val pipedRDD = rdd.map { sdf =>
      ConformerPipeline.pipeString(sdf,
        List(SparkFiles.get(dockingstdFileName),"--receptor",
          SparkFiles.get(receptorFileName), "--config", SparkFiles.get(confFileName)))
    }
    pipedRDD
  }

  private def sdfStringToIAtomContainer(sdfRecord: String) = {

    val it = SBVSPipeline.CDKInit(sdfRecord)
    var res = Seq[(IAtomContainer)]()
    while (it.hasNext()) {
      //for each molecule in the record compute the signature
      val mol = it.next
      res = res ++ Seq(mol)
    }

    res //return the molecule
  }

  private def writeSignature(sdfRecord: String, signature: String) = {
    val it = SBVSPipeline.CDKInit(sdfRecord)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    while (it.hasNext()) {
      val mol = it.next
      mol.setProperty("Signature", signature)
      mol.removeProperty("cdk:Remark")
      writer.write(mol)
    }
    writer.close
    strWriter.toString() //return the molecule  
  }

}

private[vs] class ConformerPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformerTransforms {

  override def dock(receptorPath: String, dockTimePerMol: Boolean) = {
    val pipedRDD = ConformerPipeline.getDockingRDD(receptorPath, dockTimePerMol, sc, rdd)
    val res = pipedRDD.flatMap(SBVSPipeline.splitPDBmolecules)
    new PosePipeline(res)
  }

  override def generateSignatures = {
    //Split molecules, so there is only one molecule per RDD record
    val splitRDD = rdd.flatMap(SBVSPipeline.splitSDFmolecules)
    //Convert to IAtomContainer, fake labels are added
    val molsWithFakeLabels = splitRDD.flatMap {
      case (sdfmol) =>
        ConformerPipeline.sdfStringToIAtomContainer(sdfmol)
          .map {
            case (mol) =>
              //Make sg library happy
              (sdfmol, 0.0, mol) // sdfmol is a carry, 0.0 is fake label and mol is the IAtomContainer
          }
    }
    //Convert to labeled point 
    val (lps, _) = SGUtils.atoms2LP_UpdateSignMapCarryData(molsWithFakeLabels, null, 1, 3)
    //Throw away the labels and only keep the features 
    val molAndSparseVector = lps.map {
      case (mol, lp) => (mol, lp.features.toSparse.toString())
    }
    //Write Signature in the SDF String
    val res = molAndSparseVector.map {
      case (mol, sign) => ConformerPipeline.writeSignature(mol, sign)
    }
    new ConformersWithSignsPipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}
