package se.uu.farmbio.vs.examples

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.PrintWriter
import java.sql.DriverManager
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.openscience.cdk.DefaultChemObjectBuilder
import org.openscience.cdk.io.iterator.IteratingSDFReader
import scopt.OptionParser
import se.uu.farmbio.vs.MLlibSVM
import se.uu.it.cp.InductiveClassifier
import java.lang.Long
import se.uu.farmbio.sg.types.Sig2ID_Mapping
import se.uu.farmbio.sg.SGUtils
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline
import java.io.PrintWriter
import org.openscience.cdk.io.MDLReader
import org.openscience.cdk.io.MDLV2000Reader
import java.io.Reader
/**
 * @author laeeq
 */

object StandalonePrediction {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    sig2IdPath: String = null,
    filePath: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("StandalonePrediction") {
      head("Predicting ligands with ready-made model")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<sig2IdMap-file-Path>")
        .required()
        .text("path for loading old sig2Id Mapping File")
        .action((x, c) => c.copy(sig2IdPath = x))
      arg[String]("<predictions-file-Path>")
        .required()
        .text("path to predictions")
        .action((x, c) => c.copy(filePath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {

    //Init Spark
    val conf = new SparkConf()
      .setAppName("SignatureExample")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)

    //Reading new molecules
    val sdfFile = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)

    //Loading old_sig2ID Mapping
    val old_sig2ID = SGUtils.loadSign2IDMapping(sc, params.sig2IdPath)

    //Generate Signature of New Molecule
    val signatures = sdfFile.generateNewSignatures(old_sig2ID)
    .flatMap { mol => SBVSPipeline.splitSDFmolecules(mol) }
    .collect().toString()
    
    
    
    val ins = this.getClass().getClassLoader().getResourceAsStream(signatures);
    //val reader1 = new MDLV2000Reader(ins);
    /*
    val pw1 = new PrintWriter("data/signatures.sdf")
    signatures.foreach(pw1.println(_))
    pw1.close
*/
    //Creating Input Stream for IteratingSDFReader 
    //val reader = signatures.map { case signs =>  SBVSPipeline.CDKInit(signs)}
    //val ins = this.getClass().getClassLoader().getResourceAsStream(signatures)
     
    //Creating IteratingSDFReader for reading molecules
    val reader = new IteratingSDFReader(
      ins, DefaultChemObjectBuilder.getInstance())

    //Reading Signatures and converting them to vector 
    var res = Seq[(Vector)]()
    
    while (reader.hasNext()) {
      val mol = reader.next()
      val Vector = Vectors.parse(mol.getProperty("Signature"))
      res = res ++ Seq(Vector)
    }

    //Load Model
    val svmModel = loadModel()

    //Predict New molecule(s)
    val predictions = res.map(features => (features, svmModel.predict(features.toArray, 0.5)))

    //Update Predictions to the Prediction Table
    val pw = new PrintWriter(params.filePath)
    predictions.foreach(pw.println(_))
    pw.close

  }

  def loadModel() = {
    //Connection Initialization
    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://localhost:3306/db_profile?user=root&password=2264421_root"
    val connection = DriverManager.getConnection(jdbcUrl)

    //Reading Pre-trained model from Database
    var model: InductiveClassifier[MLlibSVM, LabeledPoint] = null
    if (!(connection.isClosed())) {

      val sqlRead = connection.prepareStatement("SELECT r_model FROM MODELS")
      val rs = sqlRead.executeQuery()
      rs.next()

      val modelStream = rs.getObject("r_model").asInstanceOf[Array[Byte]]
      val modelBaip = new ByteArrayInputStream(modelStream)
      val modelOis = new ObjectInputStream(modelBaip)
      model = modelOis.readObject().asInstanceOf[InductiveClassifier[MLlibSVM, LabeledPoint]]

      rs.close
      sqlRead.close
      connection.close()
    } else {
      println("MariaDb Connection is Close")
      System.exit(1)
    }
    model
  }

}
