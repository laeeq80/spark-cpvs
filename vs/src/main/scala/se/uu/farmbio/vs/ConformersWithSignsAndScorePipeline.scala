package se.uu.farmbio.vs

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.StringWriter
import java.nio.file.Paths
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.openscience.cdk.io.SDFWriter

import se.uu.it.cp
import se.uu.it.cp.ICP
import se.uu.it.cp.InductiveClassifier

trait ConformersWithSignsAndScoreTransforms {
  def dockWithML(
    receptorPath: String,
    pdbCode: String,
    jdbcHostname: String,
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

  private[vs] def parseIdAndSignature(poseWithSigns: String) = {
    var signature: String = null
    val id: String = PosePipeline.parseId(poseWithSigns)
    //Sometimes OEChem produce molecules with empty score or malformed molecules
    //We use try catch block for those exceptions

    var res: String = null
    val it = SBVSPipeline.CDKInit(poseWithSigns)
    if (it.hasNext()) {
      val mol = it.next
      res = mol.getProperty("Signature")

    }
    signature = res

    (id, signature)

  }

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

  private def insertPredictions(receptorPath: String, r_pdbCode: String, jdbcHostname: String, predictions: RDD[(String, Set[Double])], sc: SparkContext) {
    //Reading receptor name from path
    val r_name = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())

    //Getting parameters ready in Row format
    val paramsAsRow = predictions.map {
      case (sdfmol, predSet) =>
        val l_id = PosePipeline.parseId(sdfmol)
        val l_prediction = if (predSet == Set(0.0)) "BAD"
        else if (predSet == Set(1.0)) "GOOD"
        else "UNKNOWN"
        (l_id, l_prediction)
    }.map {
      case (l_id, l_prediction) =>
        Row(r_name, r_pdbCode, l_id, l_prediction)
    }

    //Creating sqlContext Using sparkContext  
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schema =
      StructType(
        StructField("r_name", StringType, false) ::
          StructField("r_pdbCode", StringType, false) ::
          StructField("l_id", StringType, false) ::
          StructField("l_prediction", StringType, false) :: Nil)

    //Creating DataFrame using row parameters and schema
    val df = sqlContext.createDataFrame(paramsAsRow, schema)

    val prop = new java.util.Properties
    prop.setProperty("driver", "org.mariadb.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "2264421_root")

    //jdbc mysql url - destination database is named "db_profile"
    val url = "jdbc:mysql://" + jdbcHostname + ":3306/db_profile"

    //destination database table 
    val table = "PREDICTED_LIGANDS"

    //write data from spark dataframe to database
    df.write.mode("append").jdbc(url, table, prop)
    df.printSchema()
  }

  private def insertModels(receptorPath: String, r_model: InductiveClassifier[MLlibSVM, LabeledPoint], r_pdbCode: String, jdbcHostname: String) {
    //Getting filename from Path and trimming the extension
    val r_name = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())
    println("JOB_INFO: The value of r_name is " + r_name)

    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://" + jdbcHostname + ":3306/db_profile?user=root&password=2264421_root"

    //Preparation object for writing
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(r_model)

    val r_modelAsBytes = baos.toByteArray()
    val bais = new ByteArrayInputStream(r_modelAsBytes)

    val connection = DriverManager.getConnection(jdbcUrl)
    if (!(connection.isClosed())) {
      //Writing to Database
      val sqlInsert: PreparedStatement = connection.prepareStatement("INSERT INTO MODELS(r_name, r_pdbCode, r_model) VALUES (?, ?, ?)")

      println("JOB_INFO: Start Serializing")

      // set input parameters
      sqlInsert.setString(1, r_name)
      sqlInsert.setString(2, r_pdbCode)
      sqlInsert.setBinaryStream(3, bais, r_modelAsBytes.length)
      sqlInsert.executeUpdate()

      sqlInsert.close()
      println("JOB_INFO: Done Serializing")

    } else {
      println("MariaDb Connection is Close")
      System.exit(1)
    }
  }

}

private[vs] class ConformersWithSignsAndScorePipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsAndScoreTransforms {

  override def dockWithML(
    receptorPath: String,
    pdbCode: String,
    jdbcHostname: String,
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
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules)
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
        dsInit = ds.sample(false, dsInitSize / ds.count.toDouble)
      else
        dsInit = ds.sample(false, dsIncreSize / ds.count.toDouble)

      //Step 2
      //Subtract the sampled molecules from main dataset
      ds = ds.subtract(dsInit)

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

      //Converting SDF training set to LabeledPoint(label+features) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsAndScorePipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      //calibrationSizeDynamic = (dsTrain.count * calibrationPercent).toInt
      val Array(properTraining, calibration) = lpDsTrain.randomSplit(Array(1 - calibrationPercent, calibrationPercent), seed = 11L)

      //Train ICP  
      val svm = new MLlibSVM(properTraining.cache, numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, nOfClasses = 2, calibration.collect)

      //ConformersWithSignsAndScorePipeline.insertModels(receptorPath, icp, pdbCode, jdbcHostname)

      lpDsTrain.unpersist()
      properTraining.unpersist()

      //Step 9 Prediction using our model
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData.toArray, confidence))
      }

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

      ds = ds.subtract(dsZeroRemoved)
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

      dsInit.unpersist()

      //ConformersWithSignsAndScorePipeline.insertPredictions(receptorPath, pdbCode, jdbcHostname, predictions, sc)

      counter = counter + 1
      if (eff > 0.8)
        effCounter = effCounter + 1
      else
        effCounter = 0
      if (effCounter >= 2) {
        ConformersWithSignsAndScorePipeline.insertModels(receptorPath, icp, pdbCode, jdbcHostname)
        ConformersWithSignsAndScorePipeline.insertPredictions(receptorPath, pdbCode, jdbcHostname, predictions, sc)
      }

    } while (effCounter < 2 && !singleCycle)
    logInfo("JOB_INFO: Total number of bad mols removed are " + cumulativeZeroRemoved.count)

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