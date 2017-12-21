package se.uu.farmbio.vs

import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.mllib.classification.{ SVMWithSGD, SVMModel }

import org.apache.spark.mllib.optimization.{ LBFGS, HingeGradient, SquaredL2Updater }

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import se.uu.it.cp.UnderlyingAlgorithm

// Define a MLlib SVM underlying algorithm
private object SVM {
  def trainingProcedure(
    input: RDD[LabeledPoint],
    maxNumItearations: Int,
    regParam: Double,
    numCorrections: Int,
    convergenceTol: Double) = {

    //Train SVM with LBFGS
    val numFeatures = input.take(1)(0).features.size
    val training = input.map(x => (x.label, MLUtils.appendBias(x.features))).cache()
    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
    val (weightsWithIntercept, _) = LBFGS.runLBFGS(
      training,
      new HingeGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumItearations,
      regParam,
      initialWeightsWithIntercept)

    //Create the model using the weights
    val model = new SVMModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1))

    //Return raw score predictor
    model.clearThreshold()
    model

  }
}

private[vs] class MLlibSVM(val properTrainingSet: RDD[LabeledPoint], numIterations: Int)
    extends UnderlyingAlgorithm[LabeledPoint] {

  // First describe how to access Spark's LabeledPoint structure 
  override def makeDataPoint(features: Seq[Double], label: Double) =
    new LabeledPoint(label, Vectors.dense(features.toArray))
  override def getDataPointFeatures(lp: LabeledPoint) = lp.features.toArray
  override def getDataPointLabel(lp: LabeledPoint) = lp.label

  val regParam: Double = 0.1
  val numCorrections: Int = 10
  val convergenceTol: Double = 1e-4
  val svmModel = SVM.trainingProcedure(properTrainingSet, numIterations, regParam, numCorrections, convergenceTol)

  // Define nonconformity measure as signed distance from the dividing hyperplane
  override def nonConformityMeasure(lp: LabeledPoint) = {
    if (lp.label == 1.0) {
      -svmModel.predict(lp.features)
    } else {
      svmModel.predict(lp.features)
    }
  }

}