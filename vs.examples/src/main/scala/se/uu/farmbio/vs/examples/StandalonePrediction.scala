package se.uu.farmbio.vs.examples

import java.io.{ ByteArrayInputStream, ObjectInputStream, FileInputStream, PrintWriter, File }
import java.sql.DriverManager

import org.apache.spark.mllib.regression.LabeledPoint
import org.openscience.cdk.DefaultChemObjectBuilder
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.iterator.IteratingSDFReader

import scopt.OptionParser
import se.uu.farmbio.vs.{ MLlibSVM, SGUtils_Serial }

import se.uu.it.cp.InductiveClassifier

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
    //New molecules
    val sdfFile = new File(params.conformersFile);

    //Loading oldSig2ID Mapping
    val oldSig2ID = SGUtils_Serial.loadSig2IdMap(params.sig2IdPath)

    //Creating IteratingSDFReader for reading molecules
    val reader = new IteratingSDFReader(
      new FileInputStream(sdfFile), DefaultChemObjectBuilder.getInstance())

    //Getting Seq of IAtomContainer from reader
    var res = Seq[(IAtomContainer)]()
    while (reader.hasNext()) {
      //for each molecule in the record compute the signature
      val mol = reader.next
      res = res ++ Seq(mol)
    }

    //Array of IAtomContainers
    val iAtomArray = res.toArray

    //Unit sent as carry, later we can add any type required
    val iAtomArrayWithFakeCarry = iAtomArray.map { case x => (Unit, x) }

    //Generate Signature(in vector form) of New Molecule(s)
    val newSigns = SGUtils_Serial.atoms2LP_carryData(iAtomArrayWithFakeCarry, oldSig2ID, 1, 3)

    //Load Model
    val svmModel = loadModel()

    //Predict New molecule(s)
    val predictions = newSigns.map { case (sdfMols, features) => (features, svmModel.predict(features.toArray, 0.5)) }

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