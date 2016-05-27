package se.uu.farmbio.vs

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkFiles
import org.apache.commons.lang.NotImplementedException

import java.lang.Exception
import java.io.PrintWriter
import java.nio.file.Paths

import scala.io.Source
import scala.collection.JavaConverters.seqAsJavaListConverter


trait ConformersWithSignsTransforms {
  def dockWithML(receptorPath: String, method: Int, resolution: Int): SBVSPipeline
}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms{
      
 override def dockWithML(receptorPath: String, method: Int, resolution: Int) = {
    //We need to dock some percent of the whole dataset to get idea of good molecules
    val pipedRDD = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, sc, rdd)
    
    //removes empty molecule caused by oechem optimization problem
    val cleanedRDD = pipedRDD.map(_.trim).filter(_.nonEmpty)        
    val poseRDD = new PosePipeline(cleanedRDD)
    val sortedRDD = poseRDD.sortByScore
    //Labeling
    //Training
    //Prediction
  }

  
}