package se.uu.farmbio.vs

import scala.io.Source

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with PoseTransforms
  def getTopPoses(topN: Int): Array[String]
}

private[vs] object PosePipeline extends Logging {

  private def parseId(pose: String) = {
    Source.fromString(pose).getLines.next
  }

  private[vs] def parseIdAndScore(pose: String) = {
    var score: Double = Double.MinValue
    val id: String = parseId(pose)
    //Sometimes OEChem produce molecules with empty score or malformed molecules
    //We use try catch block for those exceptions
    
      
      var res: String = null
      val it = SBVSPipeline.CDKInit(pose)
      if (it.hasNext()) {
        val mol = it.next
        res = mol.getProperty("Chemgauss4")

      }
      score = res.toDouble
    
    (id, score)

  }

  private[vs] def parseScore(pose: String) = {
    var result: Double = Double.MinValue
    //Sometimes OEChem produce molecules with empty score or malformed molecules
    //We use try catch block for those exceptions
   
      var res: String = null
      val it = SBVSPipeline.CDKInit(pose)
      if (it.hasNext()) {
        val mol = it.next
        res = mol.getProperty("Chemgauss4")
      }
      result = res.toDouble
    
    result
  }

  @deprecated("parent Method collapse deprecated", "Sep 29, 2016")
  private def collapsePoses(bestN: Int, parseScore: String => Double) = (record: (String, Iterable[String])) => {
    record._2.toList.sortBy(parseScore).reverse.take(bestN)
  }

}

private[vs] class PosePipeline(override val rdd: RDD[String]) extends SBVSPipeline(rdd)
    with PoseTransforms {

  // Need a local copy due to serialization error 
  // http://spark-summit.org/wp-content/uploads/2013/10/McDonough-spark-tutorial_spark-summit-2013.pptx
  //val methodBroadcast = rdd.sparkContext.broadcast(scoreMethod)

  override def getTopPoses(topN: Int) = {
    val cachedRDD = rdd.cache()
    //val methodBroadcastLocal = methodBroadcast
    //val method = methodBroadcastLocal.value
    //Parsing id and Score in parallel and collecting data to driver
    val idAndScore = cachedRDD.map {
      case (mol) => PosePipeline.parseIdAndScore(mol)
    }.collect()

    //Finding Distinct top id and score in serial at driver
    val topMols =
      idAndScore.foldLeft(Map[String, Double]() withDefaultValue Double.MinValue) {
        case (m, (id, score)) => m updated (id, score max m(id))
      }
        .toSeq
        .sortBy { case (id, score) => -score }
        .take(topN).toArray

    //Broadcasting the top id and score and search main rdd
    //for top molecules in parallel  
    val topMolsBroadcast = cachedRDD.sparkContext.broadcast(topMols)
    val topPoses = cachedRDD.filter { mol =>
      val idAndScore = PosePipeline.parseIdAndScore(mol)
      topMolsBroadcast.value
        .map(topHit => topHit == idAndScore)
        .reduce(_ || _)
    }
    //return statement  
    topPoses.collect
      .sortBy {
        mol => -PosePipeline.parseScore(mol)
      }
  }
  
  
  @deprecated("Spark sortBy is slow, use getTopPoses instead", "Sep 29, 2016")
  override def sortByScore = {
    val res = rdd.sortBy(PosePipeline
      .parseScore, false)
    new PosePipeline(res)
  }

  @deprecated("getTopPoses includes collapsing", "Sep 29, 2016")
  override def collapse(bestN: Int) = {
    val res = rdd.groupBy(PosePipeline.parseId)
      .flatMap(PosePipeline.collapsePoses(bestN, PosePipeline.parseScore))
    new PosePipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new PosePipeline(res)
  }

}