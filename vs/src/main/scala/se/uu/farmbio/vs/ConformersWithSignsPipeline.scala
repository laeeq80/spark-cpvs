package se.uu.farmbio.vs

import java.io.StringWriter
import java.nio.file.Paths
import java.sql.DriverManager
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.openscience.cdk.io.SDFWriter
import org.apache.spark.sql.Row

import org.apache.spark.sql.types._

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream


import se.uu.it.cp
import se.uu.it.cp.ICP
import se.uu.it.cp.InductiveClassifier
import java.sql.PreparedStatement

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath: String,
    pdbCode: String,
    jdbcHostname : String,
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

  private def insertPredictions(receptorPath: String, rPdbCode: String, jdbcHostname : String, predictions: RDD[(String, Set[Double])], sc: SparkContext) {
    //Reading receptor name from path
    val rName = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())

    //Getting parameters ready in Row format
    val paramsAsRow = predictions.map {
      case (sdfmol, predSet) =>
        val lId = PosePipeline.parseId(sdfmol)
        val lPrediction = if (predSet == Set(0.0)) "BAD"
        else if (predSet == Set(1.0)) "GOOD"
        else "UNKNOWN"
        (lId, lPrediction)
    }.map {
      case (lId, lPrediction) =>
        Row(rName, rPdbCode, lId, lPrediction)
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
  
private def insertModels(receptorPath: String, rModel: InductiveClassifier[MLlibSVM, LabeledPoint], rPdbCode: String , jdbcHostname : String) {
    //Getting filename from Path and trimming the extension
    val rName = FilenameUtils.removeExtension(Paths.get(receptorPath).getFileName.toString())
    println("JOB_INFO: The value of rName is " + rName)

    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://" + jdbcHostname + ":3306/db_profile?user=root&password=2264421_root"
    
    //Preparation object for writing
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(rModel)

    val rModelAsBytes = baos.toByteArray()
    val bais = new ByteArrayInputStream(rModelAsBytes)

    val connection = DriverManager.getConnection(jdbcUrl)
    if (!(connection.isClosed())) {
      //Writing to Database
      val sqlInsert: PreparedStatement = connection.prepareStatement("INSERT INTO MODELS(r_name, r_pdbCode, r_model) VALUES (?, ?, ?)")

      println("JOB_INFO: Start Serializing")

      // set input parameters
      sqlInsert.setString(1, rName)
      sqlInsert.setString(2, rPdbCode)
      sqlInsert.setBinaryStream(3, bais, rModelAsBytes.length)
      sqlInsert.executeUpdate()

      sqlInsert.close()
      println("JOB_INFO: Done Serializing")

    } else {
      println("MariaDb Connection is Close")
      System.exit(1)
    }
  }

}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  override def dockWithML(
    receptorPath: String,
    pdbCode: String,
    jdbcHostname: String,
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
    var dsTrain: RDD[String] = null
    var dsOnePredicted: RDD[(String)] = null
    var ds: RDD[String] = rdd.flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.MEMORY_AND_DISK_SER)
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
        dsInit = ds.sample(false, dsInitSize / ds.count.toDouble)
      else
        dsInit = ds.sample(false, dsIncreSize / ds.count.toDouble)

      logInfo("JOB_INFO: Sample taken for docking in cycle " + counter)

      val dsInitToDock = dsInit.mapPartitions(x => Seq(x.mkString("\n")).iterator)

      //Step 2
      //Docking the sampled dataset
      val dsDock = ConformerPipeline
        .getDockingRDD(receptorPath, method, resolution, dockTimePerMol = false, sc, dsInitToDock)
        //Removing empty molecules caused by oechem optimization problem
        .flatMap(SBVSPipeline.splitSDFmolecules).persist(StorageLevel.DISK_ONLY)

      logInfo("JOB_INFO: Docking Completed in cycle " + counter)

      //Step 3
      //Subtract the sampled molecules from main dataset
      ds = ds.subtract(dsInit)

      //Step 4
      //Keeping processed poses
      if (poses == null) {
        poses = dsDock
      } else {
        poses = poses.union(dsDock)
      }

      //Step 5 and 6 Computing dsTopAndBottom
      val parseScoreRDD = dsDock.map(PosePipeline.parseScore(method)).persist(StorageLevel.MEMORY_ONLY)
      val parseScoreHistogram = parseScoreRDD.histogram(10)
      
      val dsTopAndBottom = dsDock.map {
        case (mol) =>
          val score = PosePipeline.parseScore(method)(mol)
          ConformersWithSignsPipeline.labelTopAndBottom(mol, score, parseScoreHistogram._1, badIn, goodIn)
      }.map(_.trim).filter(_.nonEmpty)
      
      //Step 7 Union dsTrain and dsTopAndBottom
      if (dsTrain == null) {
        dsTrain = dsTopAndBottom
      } else {
        dsTrain = dsTrain.union(dsTopAndBottom).persist(StorageLevel.DISK_ONLY)
      }

      //Converting SDF training set to LabeledPoint(label+sign) required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsPipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      //Splitting data into Proper training set and calibration set
      val Array(properTraining, calibration) = lpDsTrain.randomSplit(Array(1 - calibrationPercent, calibrationPercent), seed = 11L)

      //Train ICP  
      val svm = new MLlibSVM(properTraining.persist(StorageLevel.MEMORY_AND_DISK_SER), numIterations)
      //SVM based ICP Classifier (our model)
      val icp = ICP.trainClassifier(svm, nOfClasses = 2, calibration.collect)

      parseScoreRDD.unpersist()
      lpDsTrain.unpersist()
      properTraining.unpersist()

      logInfo("JOB_INFO: Training Completed in cycle " + counter)

      //Step 10 Prediction using our model on complete dataset
      val predictions = fvDsComplete.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData.toArray, confidence))
      }

      val dsZeroPredicted: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }

      //Step 11 Subtracting {0} mols from main dataset
      ds = ds.subtract(dsZeroPredicted).persist(StorageLevel.MEMORY_AND_DISK_SER)

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
      if (effCounter >= 2) {
        dsOnePredicted = predictions
          .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
          .map { case (sdfmol, prediction) => sdfmol }
        ConformersWithSignsPipeline.insertModels(receptorPath, icp, pdbCode, jdbcHostname)
        ConformersWithSignsPipeline.insertPredictions(receptorPath, pdbCode, jdbcHostname, predictions, sc)
      }
    } while (effCounter < 2 && !singleCycle)

    dsOnePredicted = dsOnePredicted.subtract(poses)

    val dsOnePredictedToDock = dsOnePredicted.mapPartitions(x => Seq(x.mkString("\n")).iterator)

    val dsDockOne = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, false, sc, dsOnePredictedToDock)
      //Removing empty molecules caused by oechem optimization problem
      .flatMap(SBVSPipeline.splitSDFmolecules)

    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)
    new PosePipeline(poses, method)
  }

}