package se.uu.farmbio.vs.examples

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.PrintWriter
import java.sql.DriverManager

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.openscience.cdk.DefaultChemObjectBuilder
import org.openscience.cdk.io.iterator.IteratingSDFReader

import scopt.OptionParser
import se.uu.farmbio.vs.MLlibSVM
import se.uu.it.cp.InductiveClassifier

/**
 * @author laeeq
 */

object StandalonePrediction{

  case class Arglist(
    conformersFile: String = null,
    filePath: String = null)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("StandalonePrediction") {
      head("Predicting ligands with ready-made model")
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<single-file-Path>")
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

    val sdfFile = new File(params.conformersFile);
    val reader = new IteratingSDFReader(
      new FileInputStream(sdfFile), DefaultChemObjectBuilder.getInstance())

    var res = Seq[(Vector)]()

    //Reading Signatures and converting them to vector 
    while (reader.hasNext()) {
      val mol = reader.next()
      val Vector = Vectors.parse(mol.getProperty("Signature"))
      res = res ++ Seq(Vector)
    }

    val icp = readModel()

    val predictions = res.map(features => (features, icp.predict(features.toArray, 0.5)))

    val pw = new PrintWriter(params.filePath)
    predictions.foreach(pw.println(_))
    pw.close

  }

  def readModel() = {
    //Reading Pre-trained model from Database
    Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://localhost:3306/db_profile?user=root&password=2264421_root"

    val connection = DriverManager.getConnection(jdbcUrl)

    var model: InductiveClassifier[MLlibSVM, LabeledPoint] = null
    if (!(connection.isClosed())) {

      val sqlRead = connection.prepareStatement("SELECT r_model FROM MODELS")
      val rs = sqlRead.executeQuery()
      rs.next()

      val modelStream = rs.getObject("r_model").asInstanceOf[Array[Byte]]
      val baip = new ByteArrayInputStream(modelStream)
      val ois = new ObjectInputStream(baip)
      model = ois.readObject().asInstanceOf[InductiveClassifier[MLlibSVM, LabeledPoint]]

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
