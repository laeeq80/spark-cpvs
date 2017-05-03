package se.uu.farmbio.vs

import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.alg.SVM
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.openscience.cdk.io.SDFWriter
import java.io.StringWriter
import org.apache.spark.storage.StorageLevel

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath: String,
    method: Int,
    resolution: Int,
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

object ConformersWithSignsPipeline extends Serializable {

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

  private def labelTopAndBottom(sdfRecord: String,
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

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  override def dockWithML(
    receptorPath: String,
    method: Int,
    resolution: Int,
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
    var posesTemp: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsRemaining: RDD[String] = null
    var trainTemp: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var dsTemp: RDD[String] = null
    var eff: Double = 0.0
    var counter: Int = 1
    var effCounter: Int = 0
    var calibrationSizeDynamic: Int = 0
    var dsInit: RDD[String] = null

    //Converting complete dataset (dsComplete) to feature vector required for conformal prediction
    //We also need to keep intact the poses so at the end we know
    //which molecules are predicted as bad and remove them from main set

    val fvDsComplete = ds.flatMap {
      sdfmol =>
        ConformersWithSignsPipeline.getFeatureVector(sdfmol)
          .map { case (vector) => (sdfmol, vector) }
    }

    do {
      //Step 1
      //Get a sample of the data
      if (dsInit == null)
        dsInit = ds.sample(false, dsInitSize / ds.count().toDouble)
      else
        dsInit = dsRemaining.sample(false, dsIncreSize / dsRemaining.count().toDouble)
      logInfo("JOB_INFO: Sample taken for docking in cycle " + counter)

      //Step 2
      //Docking the sampled dataset
      val dsDock = ConformerPipeline
        .getDockingRDD(receptorPath, method, resolution, dockTimePerMol = false, sc, dsInit)
        //Removing empty molecules caused by oechem optimization problem
        .map(_.trim).filter(_.nonEmpty).persist(StorageLevel.MEMORY_AND_DISK_SER)
      logInfo("JOB_INFO: Docking Completed in cycle " + counter)

      //Step 3
      //Keeping processed poses
      if (poses == null) {
        posesTemp = dsDock.persist(StorageLevel.MEMORY_AND_DISK_SER)
      } else {
        posesTemp = poses.union(dsDock).persist(StorageLevel.MEMORY_AND_DISK_SER)
        poses.unpersist()
      }
      poses = posesTemp.persist(StorageLevel.MEMORY_AND_DISK_SER)
      posesTemp.unpersist()

      //Step 4 and 5 Computing dsTopAndBottom
      val parseScoreRDD = dsDock.map(PosePipeline.parseScore(method))
      val parseScoreHistogram = parseScoreRDD.histogram(10)

      val dsTopAndBottom = dsDock.map {
        case (mol) =>
          val score = PosePipeline.parseScore(method)(mol)
          ConformersWithSignsPipeline.labelTopAndBottom(mol, score, parseScoreHistogram._1, badIn, goodIn)
      }.map(_.trim).filter(_.nonEmpty)

      //Step 6 Union dsTrain and dsTopAndBottom
      if (dsTrain == null) {
        trainTemp = dsTopAndBottom
      } else {
        trainTemp = dsTrain.union(dsTopAndBottom).persist(StorageLevel.MEMORY_ONLY_SER)
        dsTrain.unpersist()
      }
      dsTrain = trainTemp.persist(StorageLevel.MEMORY_ONLY_SER)
      trainTemp.unpersist()

      //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsPipeline.getLPRDD(sdfmol)
      }

      //Step 7 Training
      //Train icps
      calibrationSizeDynamic = (dsTrain.count * calibrationPercent).toInt
      val (calibration, properTraining) = ICP.calibrationSplit(
        lpDsTrain.persist(StorageLevel.MEMORY_ONLY_SER), calibrationSizeDynamic, stratified)

      //Train ICP
      val svm = new SVM(properTraining.persist(StorageLevel.MEMORY_ONLY_SER), numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, numClasses = 2, calibration)
      lpDsTrain.unpersist()
      properTraining.unpersist()

      logInfo("JOB_INFO: Training Completed in cycle " + counter)

      //Step 8 Prediction using our model on complete dataset
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData, confidence))
      }

      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }

      //Step 9 Subtracting the already docked molecules from main dataset
      // and {0} mols             
      if (dsRemaining == null) {
        dsTemp = ds.subtract(dsInit.union(dsZeroPredicted))
      } else {
        dsTemp = dsRemaining.subtract(dsInit.union(dsZeroPredicted)).persist(StorageLevel.MEMORY_AND_DISK_SER)
        dsRemaining.unpersist()
      }
      dsRemaining = dsTemp.persist(StorageLevel.MEMORY_AND_DISK_SER)
      dsTemp.unpersist()
      //Computing efficiency for stopping loop
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

      if (eff >= 0.8) {
        effCounter = effCounter + 1
      } else {
        effCounter = 0
      }
      counter = counter + 1
      if (eff >= 2) {
        dsOnePredicted = predictions
          .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
          .map { case (sdfmol, prediction) => sdfmol }.persist(StorageLevel.MEMORY_ONLY_SER)
      }

      dsDock.unpersist()
    } while (effCounter < 2 && !singleCycle)

    dsOnePredicted = dsOnePredicted.subtract(poses)
    val dsDockOne = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, false, sc, dsOnePredicted)
      //Removing empty molecules caused by oechem optimization problem
      .flatMap(SBVSPipeline.splitSDFmolecules).filter(_.nonEmpty)

    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)
    logInfo("JOB_INFO: Total number of docked mols are " + poses.count)
    new PosePipeline(poses, method)
  }

}