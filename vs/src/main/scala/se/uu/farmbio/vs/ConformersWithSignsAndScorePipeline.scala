package se.uu.farmbio.vs

import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.alg.SVM
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.openscience.cdk.io.SDFWriter
import java.io.StringWriter
import se.uu.farmbio.cp.alg.SVM

trait ConformersWithSignsAndScoreTransforms {
  def dockWithML(
    dsInitSize: Int,
    calibrationSize: Int,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean): SBVSPipeline with PoseTransforms
}

private[vs] object ConformersWithSignsAndScorePipeline extends Serializable {

  def getLPRDD_Score(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(LabeledPoint)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label: String = mol.getProperty("Chemgauss4")
      val doubleLabel: Double = label.toDouble
      val labeledPoint = new LabeledPoint(doubleLabel, Vectors.parse(mol.getProperty("Signature")))
      res = res ++ Seq(labeledPoint)
    }

    res //return the labeledPoint
  }

  private def getLPRDD(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(LabeledPoint)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label: String = mol.getProperty("Label")
      val doubleLabel: Double = label.toDouble
      val labeledPoint = new LabeledPoint(doubleLabel, Vectors.parse(mol.getProperty("Signature")))
      res = res ++ Seq(labeledPoint)
    }

    res //return the labeledPoint
  }

  private def getFeatureVector(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(Vector)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the FeatureVector
      val mol = it.next
      val Vector = Vectors.parse(mol.getProperty("Signature"))
      res = res ++ Seq(Vector)
    }

    res //return the FeatureVector
  }

  private def labelTopAndBottom(
    sdfRecord: String,
    score: Double,
    scoreHistogram: Array[Double],
    badIn: Int,
    goodIn: Int) = {
    val it = SBVSPipeline.CDKInit(sdfRecord)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    while (it.hasNext()) {
      val mol = it.next
      val label = score match { //convert labels
        case score if score >= scoreHistogram(0) && score <= scoreHistogram(badIn) => 0.0
        case score if score >= scoreHistogram(goodIn) && score <= scoreHistogram(10) => 1.0
        case _ => "NAN"
      }

      if (label == 0.0 || label == 1.0) {
        mol.removeProperty("cdk:Remark")
        mol.setProperty("Label", label)
        writer.write(mol)
      }
    }
    writer.close
    strWriter.toString() //return the molecule  
  }

}

private[vs] class ConformersWithSignsAndScorePipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsAndScoreTransforms {

  override def dockWithML(
    dsInitSize: Int,
    calibrationSize: Int,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean) = {

    //initializations
    var poses: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var dsZeroRemoved: RDD[(String)] = null
    var cumulativeZeroRemoved: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules)
    var dsComplete: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules)
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var badCounter: Int = 0

    //Converting complete dataset (dsComplete) to feature vector required for conformal prediction
    //We also need to keep intact the poses so at the end we know
    //which molecules are predicted as bad and remove them from main set

    val fvDsComplete = dsComplete.flatMap {
      sdfmol =>
        ConformersWithSignsAndScorePipeline.getFeatureVector(sdfmol)
          .map { case (vector) => (sdfmol, vector) }
    }.cache()

    do {

    //Step 1
    //Get a sample of the data
    val dsInit = sc.makeRDD(ds.takeSample(false, dsInitSize))

    //Step 2
    //Subtract the sampled molecules from main dataset
    ds = ds.subtract(dsInit)

    //Step 3
    //Mocking the sampled dataset. We already have scores, docking not required
    val dsDock = dsInit
    logInfo("\n JOB_INFO: cycle " + counter
      + "   ################################################################\n")

    logInfo("JOB_INFO: dsInit in cycle " + counter + " is " + dsInit.count)

    //Step 4
    //Keeping processed poses
    if (poses == null)
      poses = dsDock
    else
      poses = poses.union(dsDock)

    //Step 5 and 6 Computing dsTopAndBottom
    val parseScoreRDD = dsDock.map(PosePipeline.parseScore).cache
    val parseScoreHistogram = parseScoreRDD.histogram(10)

    val dsTopAndBottom = dsDock.map {
      case (mol) =>
        val score = PosePipeline.parseScore(mol)
        ConformersWithSignsAndScorePipeline.labelTopAndBottom(mol, score, parseScoreHistogram._1, badIn, goodIn)
    }.map(_.trim).filter(_.nonEmpty)

    parseScoreRDD.unpersist()
    //Step 7 Union dsTrain and dsTopAndBottom
    if (dsTrain == null)
      dsTrain = dsTopAndBottom
    else
      dsTrain = dsTrain.union(dsTopAndBottom)
    logInfo("JOB_INFO: Training set size in cycle " + counter + " is " + dsTrain.count)

    //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
    val lpDsTrain = dsTrain.flatMap {
      sdfmol => ConformersWithSignsAndScorePipeline.getLPRDD(sdfmol)
    }

    //Step 8 Training
    //Train icps
    val (calibration, properTraining) = ICP.calibrationSplit(
      lpDsTrain.coalesce(4).cache, calibrationSize)

    //Train ICP
    val svm = new SVM(properTraining.cache, numIterations)
    //SVM based ICP Classifier (our model)
    val icp = ICP.trainClassifier(svm, numClasses = 2, calibration)
    lpDsTrain.unpersist()
    properTraining.unpersist()

    //Step 9 Prediction using our model
    val predictions = fvDsComplete.map {
      case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData, 0.2))
    }

    val dsZeroPredicted: RDD[(String)] = predictions
      .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
      .map { case (sdfmol, prediction) => sdfmol }.cache
    dsOnePredicted = predictions
      .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
      .map { case (sdfmol, prediction) => sdfmol }.cache
    val dsUnknownPredicted: RDD[(String)] = predictions
      .filter { case (sdfmol, prediction) => (prediction == Set(0.0, 1.0) || prediction == Set()) }
      .map { case (sdfmol, prediction) => sdfmol }

    //Step 10 Subtracting {0} moles from dataset which has not been previously subtracted
    if (dsZeroRemoved == null)
      dsZeroRemoved = dsZeroPredicted
    else
      dsZeroRemoved = dsZeroPredicted.subtract(cumulativeZeroRemoved.union(poses))

    ds = ds.subtract(dsZeroRemoved)
    logInfo("JOB_INFO: Number of bad mols predicted in cycle " +
      counter + " are " + dsZeroPredicted.count)
    logInfo("JOB_INFO: Number of bad mols removed in cycle " +
      counter + " are " + dsZeroRemoved.count)
    logInfo("JOB_INFO: Number of good mols predicted in cycle " +
      counter + " are " + dsOnePredicted.count)
    logInfo("JOB_INFO: Number of Unknown mols predicted in cycle " +
      counter + " are " + dsUnknownPredicted.count)

    //Keeping all previous removed bad mols
    if (cumulativeZeroRemoved == null)
      cumulativeZeroRemoved = dsZeroRemoved
    else
      cumulativeZeroRemoved = cumulativeZeroRemoved.union(dsZeroRemoved)

    dsZeroPredicted.unpersist()

    //Computing efficiency for stopping
    val totalCount = sc.accumulator(0.0)
    val singletonCount = sc.accumulator(0.0)

    predictions.foreach {
      case (sdfmol, prediction) =>
        if (prediction.size == 1) {
          singletonCount += 1.0
        }
        totalCount += 1.0
    }

    eff = singletonCount.value / totalCount.value
    logInfo("JOB_INFO: Efficiency in cycle " + counter + " is " + eff)

    counter = counter + 1
    if (eff > 0.8)
      effCounter = effCounter + 1
    else
      effCounter = 0
    } while (((effCounter < 2 || counter < 5) && ds.count > 20) && !singleCycle)
    logInfo("JOB_INFO: Total number of bad mols removed are " + cumulativeZeroRemoved.count)

    //Docking rest of the dsOne mols
    val dsDockOne = dsOnePredicted.subtract(cumulativeZeroRemoved.union(poses))

    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)
    logInfo("JOB_INFO: Total number of docked mols are " + poses.count)
    new PosePipeline(poses)

  }
}