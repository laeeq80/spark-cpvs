package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import se.uu.it.cp.UnderlyingAlgorithm

// Define a MLlib SVM underlying algorithm
private[vs] class MLlibSVM(val properTrainingSet: RDD[LabeledPoint], numIterations : Int )
    extends UnderlyingAlgorithm[LabeledPoint] {

  // First describe how to access Spark's LabeledPoint structure 
  override def makeDataPoint(features: Seq[Double], label: Double) =
    new LabeledPoint(label, Vectors.dense(features.toArray))
  override def getDataPointFeatures(lp: LabeledPoint) = lp.features.toArray
  override def getDataPointLabel(lp: LabeledPoint) = lp.label

  // Train a SVM model
  val svmModel = {
    // Train with SVMWithSGD
    val svmModel = SVMWithSGD.train(properTrainingSet, numIterations = 50)
    svmModel.clearThreshold // set to return distance from hyperplane
    svmModel
  }

  // Define nonconformity measure as signed distance from the dividing hyperplane
  override def nonConformityMeasure(lp: LabeledPoint) = {
    if (lp.label == 1.0) {
      -svmModel.predict(lp.features)
    } else {
      svmModel.predict(lp.features)
    }
  }
  
}