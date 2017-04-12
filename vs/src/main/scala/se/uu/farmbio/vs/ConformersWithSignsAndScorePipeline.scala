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
import org.apache.spark.storage.StorageLevel

trait ConformersWithSignsAndScoreTransforms {
  def dockWithML(
    dsInitPercent: Double,
    incrementInPercent: Double,
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
    dsInitSize: Double,
    dsIncreSize: Double,
    calibrationPercent: Double,
    numIterations: Int,
    badIn: Int,
    goodIn: Int,
    singleCycle: Boolean,
    stratified: Boolean,
    confidence: Double) = {

    //initializations
    var poses: RDD[String] = null
    var posesTemp: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var dsZeroRemoved: RDD[(String)] = null
    var cumulativeZeroRemoved: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).cache()
    var dsTemp: RDD[String] = null
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var badCounter: Int = 0
    var dsInit: RDD[String] = null
    var calibrationSizeDynamic: Int = 0
    var dsBadInTrainingSet: RDD[String] = null
    var dsGoodInTrainingSet: RDD[String] = null
    
    //Converting complete dataset (ds) to feature vector required for conformal prediction
    //We also need to keep intact the poses so at the end we know
    //which molecules are predicted as bad and remove them from main set

    val fvDsComplete = ds.flatMap {
      sdfmol =>
        ConformersWithSignsAndScorePipeline.getFeatureVector(sdfmol)
          .map { case (vector) => (sdfmol, vector) }
    }.cache()

    do {

      //Step 1
      //Get a sample of the data
      if (dsInit == null)
        dsInit = ds.sample(false, 100/(ds.count/dsInitSize)).cache()
      else
        dsInit = ds.sample(false, 100/(ds.count/dsIncreSize)).cache()
      
      //Step 2
      //Subtract the sampled molecules from main dataset
      dsTemp = ds.subtract(dsInit).cache()
      ds.unpersist()
      
      //Step 3
      //Mocking the sampled dataset. We already have scores, docking not required
      val dsDock = dsInit
      logInfo("\nJOB_INFO: cycle " + counter
        + "   ################################################################\n")

      logInfo("JOB_INFO: dsInit in cycle " + counter + " is " + dsInit.count)
      dsInit.unpersist()
      
      //Step 4
      //Keeping processed poses
      if (poses == null)
        poses = dsDock
      else
        posesTemp = poses.union(dsDock).cache()
      poses.unpersist()
      poses = posesTemp
      
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
      dsTrain.cache()  

      //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsAndScorePipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      calibrationSizeDynamic = (dsTrain.count * calibrationPercent).toInt
      val (calibration, properTraining) = ICP.calibrationSplit(
        lpDsTrain.cache, calibrationSizeDynamic, stratified)
      dsTrain.unpersist()  
      //Train ICP
      val svm = new SVM(properTraining.cache, numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, numClasses = 2, calibration)
            
      lpDsTrain.unpersist()
      properTraining.unpersist()

      //Step 9 Prediction using our model
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData, confidence))
      }

         
      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      dsOnePredicted = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }.cache
      

      ds = dsTemp.subtract(dsZeroPredicted).cache()
      dsTemp.unpersist()

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
                
    } while (effCounter < 2 && !singleCycle)
    
    //Removing fvDsComplete from memory  
    fvDsComplete.unpersist()
      
    //Docking rest of the dsOne mols
    val dsDockOne = dsOnePredicted.subtract(poses).cache()
    logInfo("JOB_INFO: Number of mols in dsDockOne are " + dsDockOne.count)
    
    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne).cache()
    logInfo("JOB_INFO: Total number of docked mols are " + poses.count)
    new PosePipeline(poses)

  }
}