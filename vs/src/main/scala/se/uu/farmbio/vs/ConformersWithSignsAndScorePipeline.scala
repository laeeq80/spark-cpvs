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
    dsIncreSize: Int,
    calibrationPercent: Double,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean,
    stratified: Boolean,
    confidence: Double): SBVSPipeline with PoseTransforms
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

  private def getLabel(sdfRecord: String) = {

    val it = SBVSPipeline.CDKInit(sdfRecord)
    var label: String = null
    while (it.hasNext()) {
      val mol = it.next
      label = mol.getProperty("Label")

    }
    label

  }

}

private[vs] class ConformersWithSignsAndScorePipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsAndScoreTransforms {

  override def dockWithML(
    dsInitSize: Int,
    dsIncreSize: Int,
    calibrationPercent: Double,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean,
    stratified: Boolean,
    confidence: Double) = {

    //initializations
    var poses: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var dsZeroRemoved: RDD[(String)] = null
    var cumulativeZeroRemoved: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).cache()
    var dsComplete: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules)
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var badCounter: Int = 0
    var dsInit: RDD[String] = null
    var calibrationSizeDynamic: Int = 0
    var dsBadInTrainingSet: RDD[String] = null
    var dsGoodInTrainingSet: RDD[String] = null

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
      if (dsInit == null)
        dsInit = sc.makeRDD(ds.takeSample(false, dsInitSize)).cache()
      else
        dsInit = sc.makeRDD(ds.takeSample(false, dsIncreSize)).cache()

      //Step 2
      //Subtract the sampled molecules from main dataset
      ds = ds.subtract(dsInit).cache()

      //Step 3
      //Mocking the sampled dataset. We already have scores, docking not required
      val dsDock = dsInit
      logInfo("\nJOB_INFO: cycle " + counter
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

      //Counting zeroes and ones in each training set in each cycle
      if (dsTrain == null) {
        dsBadInTrainingSet = dsTopAndBottom.filter {
          case (mol) => ConformersWithSignsAndScorePipeline.getLabel(mol) == "0.0"
        }
      } else {
        dsBadInTrainingSet = dsTrain.filter {
          case (mol) => ConformersWithSignsAndScorePipeline.getLabel(mol) == "0.0"
        }
      }

      if (dsTrain == null) {
        dsGoodInTrainingSet = dsTopAndBottom.filter {
          case (mol) => ConformersWithSignsAndScorePipeline.getLabel(mol) == "1.0"
        }
      } else {
        dsGoodInTrainingSet = dsTrain.filter {
          case (mol) => ConformersWithSignsAndScorePipeline.getLabel(mol) == "1.0"
        }
      }
      logInfo("JOB_INFO: Zero Labeled Mols in Training set in cycle " + counter + " are " + dsBadInTrainingSet.count)
      logInfo("JOB_INFO: One Labeled Mols in Training set in cycle " + counter + " are " + dsGoodInTrainingSet.count)

      //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsAndScorePipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      calibrationSizeDynamic = (dsTrain.count * calibrationPercent).toInt

      val (calibration, properTraining) = ICP.calibrationSplit(
        lpDsTrain.cache, calibrationSizeDynamic, stratified)

      //Train ICP
      val svm = new SVM(properTraining.cache, numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, numClasses = 2, calibration)
      
      logInfo("JOB_INFO: Profiling model training time" + lpDsTrain.count())
      
      lpDsTrain.unpersist()
      properTraining.unpersist()

      //Step 9 Prediction using our model
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData, confidence))
      }

      logInfo("JOB_INFO: Profiling prediction time" + predictions.count())

      
      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      dsOnePredicted = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      val dsBothUnknown: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0, 1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }
      val dsEmptyUnknown: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set()) }
        .map { case (sdfmol, prediction) => sdfmol }
      
        
      //Step 10 Subtracting {0} moles from dataset which has not been previously subtracted
      if (dsZeroRemoved == null)
        dsZeroRemoved = dsZeroPredicted.subtract(poses)
      else
        dsZeroRemoved = dsZeroPredicted.subtract(cumulativeZeroRemoved.union(poses))

      ds = ds.subtract(dsZeroRemoved).cache()
      logInfo("JOB_INFO: Number of bad mols predicted in cycle " +
        counter + " are " + dsZeroPredicted.count)
      logInfo("JOB_INFO: Number of bad mols removed in cycle " +
        counter + " are " + dsZeroRemoved.count)
      logInfo("JOB_INFO: Number of good mols predicted in cycle " +
        counter + " are " + dsOnePredicted.count)
      logInfo("JOB_INFO: Number of Both Unknown mols predicted in cycle " +
        counter + " are " + dsBothUnknown.count)  
      logInfo("JOB_INFO: Number of Empty Unknown mols predicted in cycle " +
        counter + " are " + dsEmptyUnknown.count)
      

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
    } while ((effCounter < 2 && ds.count > 20) && !singleCycle)
    logInfo("JOB_INFO: Total number of bad mols removed are " + cumulativeZeroRemoved.count)
    
    ds.unpersist()
    //Docking rest of the dsOne mols
    val dsDockOne = dsOnePredicted.subtract(poses).cache()
    logInfo("JOB_INFO: Number of mols in dsDockOne are " + dsDockOne.count)
    
    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)
    logInfo("JOB_INFO: Total number of docked mols are " + poses.count)
    new PosePipeline(poses)

  }
}