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
  def dockWithML(receptorPath: String, method: Int, resolution: Int): SBVSPipeline with PosePipeline
}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms{
      
 override def dockWithML(receptorPath: String, method: Int, resolution: Int) = {
    val pipedRDD = ConformerPipeline.getPipedRDD(receptorPath, method, resolution, sc, rdd)
    val res = pipedRDD.map(_.trim).filter(_.nonEmpty) //removes empty molecule caused by oechem optimization problem       
    new PosePipeline(res)
    
  }

  
}