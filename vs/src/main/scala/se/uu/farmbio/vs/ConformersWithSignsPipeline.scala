package se.uu.farmbio.vs

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFiles
import org.apache.commons.lang.NotImplementedException

import java.lang.Exception
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Paths
import java.io.ByteArrayInputStream
import java.nio.charset.Charset

import scala.io.Source
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.math.round

import org.openscience.cdk.io.MDLV2000Reader
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import org.openscience.cdk.silent.ChemFile
import org.openscience.cdk.io.SDFWriter

import se.uu.farmbio.sg.SGUtils
import se.uu.farmbio.sg.types.SignatureRecordDecision


trait ConformersWithSignsTransforms {
  def dockWithML(receptorPath: String, method: Int, resolution: Int): SBVSPipeline
}

object ConformersWithSignsPipeline {
  
  private def writeLables = (poses: String, index: Long, molCount: Long, positiveMolPercent: Double) => {
    //get SDF as input stream
    val sdfByteArray = poses
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
    
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
  
    var MolPercent: Double = 0.0
    //mols is a Java list :-(
    val it = mols.iterator
    if (positiveMolPercent <= 0.0 || positiveMolPercent > 0.5 ) 
        MolPercent = 0.5
    else
        MolPercent = positiveMolPercent
      
    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val positiveCount = molCount * MolPercent
      val negativeCount = molCount - positiveCount
      val label = index.toDouble match { //convert labels
        case x if x <= round(positiveCount) => 1.0
        case x if x > round (negativeCount) => 0.0
        case _                              => "NAN"
      }

      mol.removeProperty("cdk:Remark")
      mol.setProperty("Label", label)
      writer.write(mol)

    }
    writer.close
    reader.close
    strWriter.toString() //return the molecule
  }
}
  
private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms{
      
 override def dockWithML(receptorPath: String, method: Int, resolution: Int) = {
    //We need to dock some percent of the whole dataset to get idea of good molecules
    val Array(dsInit,remaining) = rdd.randomSplit(Array(1.0,0.0), 1234)
    val cachedRem = remaining.cache()
    
    //Docking the small dataset
    val pipedRDD = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, sc, dsInit)
    
    //removes empty molecule caused by oechem optimization problem
    val cleanedRDD = pipedRDD.map(_.trim).filter(_.nonEmpty)        
        
    val poseRDD = new PosePipeline(cleanedRDD)
    val sortedRDD = poseRDD.sortByScore.getMolecules
    
    val molsCount = sortedRDD.count()
    val molsWithIndex = sortedRDD.zipWithIndex()
    
    //compute Lables based on percent 
    //0.3 means top 30 percent will be marked as 1.0 and last 30 as 0.0
    //Must not give a value over 0.5 or (less or equal to 0.0), 
    //if so it will be considered 0.5
    val molsAfterLabeling = molsWithIndex.map{
       case(mol, index) => ConformersWithSignsPipeline
       .writeLables(mol,index + 1,molsCount,0.6) 
      }.cache
    new PosePipeline(molsAfterLabeling)
    //Training
    //Prediction
  }

  
}