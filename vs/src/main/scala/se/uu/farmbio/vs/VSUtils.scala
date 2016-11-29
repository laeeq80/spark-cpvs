package se.uu.farmbio.vs

import org.apache.spark.rdd.RDD
import scala.util.Random

trait VSUtilsTrait {

}

private[vs] object VSUtils {

  def assignGroup(
    sdfRecord: String,
    score: Double,
    histoRange: Array[Double]) = {
    val groupId = score match { //convert labels
      case score if score >= histoRange(0) && score < histoRange(1)   => 1.0
      case score if score >= histoRange(1) && score < histoRange(2)   => 2.0
      case score if score >= histoRange(2) && score < histoRange(3)   => 3.0
      case score if score >= histoRange(3) && score < histoRange(4)   => 4.0
      case score if score >= histoRange(4) && score < histoRange(5)   => 5.0
      case score if score >= histoRange(5) && score < histoRange(6)   => 6.0
      case score if score >= histoRange(6) && score < histoRange(7)   => 7.0
      case score if score >= histoRange(7) && score < histoRange(8)   => 8.0
      case score if score >= histoRange(8) && score < histoRange(9)   => 9.0
      case score if score >= histoRange(9) && score <= histoRange(10) => 10.0
    }
    (groupId, sdfRecord)

  }

  def takeSample(idAndMol: Iterable[(Double, String)], seed: Long, n: Int) = {
    val r = new Random(seed)
    r.shuffle(idAndMol).take(n)
  }
}

private[vs] class VSUtils(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with VSUtilsTrait {

}