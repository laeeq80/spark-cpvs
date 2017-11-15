package se.uu.farmbio.vs.examples

import java.sql.DriverManager

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scopt.OptionParser

/**
 * @author laeeq
 */

object dbTest extends Logging {

  case class Arglist(
    master: String = null)

  def main(args: Array[String]) {

    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("dbTest") {
      head("Connecting MariDb and reading table Schema")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
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
      .setAppName("Take")

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Class.forName("org.mariadb.jdbc.Driver")
    val jdbcUrl = s"jdbc:mysql://localhost:3306/db_profile?user=root&password=2264421_root"

    val connection = DriverManager.getConnection(jdbcUrl)
    if (!(connection.isClosed())) {
      
      println("MariaDb Connection is Open")
      val df = sqlContext
        .read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "RECEPTORS")
        .load()

      // Looks the schema of this DataFrame.
      df.printSchema()
    } else
      println("MariaDb Connection is Close")

    sc.stop()

  }

}