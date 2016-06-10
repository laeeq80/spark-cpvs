package se.uu.farmbio.vs

import se.uu.farmbio.cp.ICPClassifierModel
import se.uu.farmbio.cp.AggregatedICPClassifier
import se.uu.farmbio.cp.BinaryClassificationICPMetrics
import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.alg.SVM

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFiles
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
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
  
  private def writeLables(
      poses: String, 
      index: Long, 
      molCount: Long, 
      positiveMolPercent: Double) = {
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
  
    var molPercent: Double = 0.0
    //mols is a Java list :-(
    val it = mols.iterator
    if (positiveMolPercent <= 0.0 || positiveMolPercent > 0.5 ) 
        molPercent = 0.5
    else
        molPercent = positiveMolPercent
      
    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val positiveCount = molCount * molPercent
      val negativeCount = molCount - positiveCount
      val label = index.toDouble match { //convert labels
        case x if x <= round(positiveCount) => 1.0
        case x if x > round (negativeCount) => 0.0
        case _                              => "NAN"
      }

      if (label == 0.0 || label == 1.0)
      {
        mol.removeProperty("cdk:Remark")
        mol.setProperty("Label", label)
        writer.write(mol)
      }
    }
    writer.close
    reader.close
    strWriter.toString() //return the molecule
  }
  
  private def getLPRDD (poses: String) = {
    //get SDF as input stream
    val sdfByteArray = poses
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
       
    //mols is a Java list :-(
    val it = mols.iterator
   
    var res = Seq[(LabeledPoint)]()
    
    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label : String = mol.getProperty("Label")
      val doubleLabel : Double = label.toDouble
      val labeledPoint = new LabeledPoint(doubleLabel,Vectors.parse(mol.getProperty("Signature")))
      res = res ++ Seq(labeledPoint)
    }

    res //return the labeledPoint
  }
     
}
  
private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms{
      
 override def dockWithML(receptorPath: String, method: Int, resolution: Int) = {
    //We need to dock some percent of the whole dataset to get idea of good molecules
    //Step 1 and 2
    val Array(dsInit,dsBag) = rdd.randomSplit(Array(1.0,0.0), 1234)
    val cachedDsBag = dsBag.cache()
    
    //Step 3
    //Docking the small dataset
    val pipedRDD = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, sc, dsInit)
    
    //removes empty molecule caused by oechem optimization problem
    val cleanedRDD = pipedRDD.map(_.trim).filter(_.nonEmpty)        
    
    
    val poseRDD = new PosePipeline(cleanedRDD)
    val sortedRDD = poseRDD.sortByScore.getMolecules
    
    //We need these two lines for the calculating the label
    val molsCount = sortedRDD.count() 
    val molsWithIndex = sortedRDD.zipWithIndex()
    
    //compute Lables based on percent 
    //0.3 means top 30 percent will be marked as 1.0 and last 30 as 0.0
    //Must not give a value over 0.5 or (less or equal to 0.0), 
    //if so it will be considered 0.5
    //Also performs Molecule filtering. Molecules labeled either 1.0 or 0.0 are retained 
    val molsAfterLabeling = molsWithIndex.map{
       case(mol, index) => ConformersWithSignsPipeline
       .writeLables(mol,index + 1,molsCount,0.1) 
      }.map(_.trim).filter(_.nonEmpty)
      
    //Converting SDF Sign+label to LabeledPoint required for cp
    val lpRDD = molsAfterLabeling.flatMap{
        sdfmol => ConformersWithSignsPipeline.getLPRDD(sdfmol)
      }
      
    //Training initializations
    val numOfICPs = 100
    val calibrationSize = 5
    val numIterations = 5
    //Train icps
    val icps = (1 to numOfICPs).map { _ =>
      val (calibration, properTraining) =
        ICP.calibrationSplit(lpRDD, calibrationSize)
      //Train ICP
      val svm = new SVM(properTraining.cache, numIterations)
      ICP.trainClassifier(svm, numClasses = 2, calibration)
    }      
    
    //Prediction  
          
    new PosePipeline(molsAfterLabeling)
    
    
  }

  
}