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
  
  private def writeLables = (poses: String, index: Long, molCount: Long) => {
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
  
    
    //mols is a Java list :-(
    val it = mols.iterator

    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label = index.toDouble match { //convert labels
        case x if x <= (molCount / 2) => 1.0
        case _                        => 0.0
      }

      mol.removeProperty("cdk:Remark")
      mol.setProperty("Label", label.toDouble)
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
    
    //Now calling the dock pipe on small part of dataset
    val pipedRDD = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, sc, dsInit)
    
    //removes empty molecule caused by oechem optimization problem
    val cleanedRDD = pipedRDD.map(_.trim).filter(_.nonEmpty)        
    val poseRDD = new PosePipeline(cleanedRDD)
    val sortedRDD = poseRDD.sortByScore.getMolecules
    
    val molsCount = sortedRDD.count()
    val molsWithIndex = sortedRDD.zipWithIndex()
    val molsAfterLabeling = molsWithIndex.map{case(mol, index) => ConformersWithSignsPipeline.writeLables(mol,index + 1,molsCount)} //Compute signatures
      .cache
    new PosePipeline(molsAfterLabeling)
    //Training
    //Prediction
  }

  
}