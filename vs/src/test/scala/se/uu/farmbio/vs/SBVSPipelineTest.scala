package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import se.uu.farmbio.parsers.SDFRecordReader
import se.uu.farmbio.parsers.SmilesRecordReader

@RunWith(classOf[JUnitRunner])
class SBVSPipelineTest extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local")
    .setAppName("SBVSPipelineTest")
    .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(SDFRecordReader.SIZE_PROPERTY_NAME, "3")
  sc.hadoopConfiguration.set(SmilesRecordReader.SIZE_PROPERTY_NAME, "3")

  ignore("sortByScore should sort a set of poses by score") {

    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("filtered_collapsed.sdf").getPath)
      .sortByScore
      .getMolecules
      .collect

    val sortedPoses = TestUtils.readSDF(getClass.getResource("filtered_collapsed_sorted.sdf").getPath)
    assert(res === sortedPoses)

  }

  ignore("collapse should collapse poses with same id to n with highest score") {

    val n = 2

    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("filtered_poses.sdf").getPath)
      .collapse(n)
      .getMolecules
      .collect

    val filteredCollapsed = TestUtils.readSDF(getClass.getResource("filtered_collapsed.sdf").getPath)
    assert(res.toSet === filteredCollapsed.toSet)

  }
  /*
  test("dock should dock a set of conformers to a receptor and generate the poses") {

    val res = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("conformers_with_failed_mol.sdf").getPath)
      .dock(getClass.getResource("receptor.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules
      .collect

    val dockedMolecules = TestUtils.readSDF(getClass.getResource("unsorted_poses.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader).toSet
      === dockedMolecules.map(TestUtils.removeSDFheader).toSet)

  }
*/
  
  test("getTopPoses should return the topN poses") {
    val topN = 10
    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("unsorted_poses.sdf").getPath)
      .getTopPoses(topN)

    val topCollapsed = TestUtils.readSDF(getClass.getResource("top_collapsed.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader) === topCollapsed.map(TestUtils.removeSDFheader))

  }
/*
  test("Signatures are maintained(not lost) after docking") {

    val molWithSigns = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("filtered_conformers.sdf").getPath)
      .generateSignatures()
      .getMolecules

    val signsBeforeDocking = molWithSigns.map {
      case (mol) =>
        TestUtils.parseSignature(mol)
    }.collect()

    val molWithSignsAndDockingScore = new SBVSPipeline(sc)
      .readConformerRDDs(Seq(molWithSigns))
      .dock(getClass.getResource("receptor.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules

    val signsAfterDocking = molWithSignsAndDockingScore.map {
      case (mol) =>
        TestUtils.parseSignature(mol)
    }.collect()

    //Comparing signatures before and after docking
    assert(signsBeforeDocking.toSet()
      === signsAfterDocking.toSet())

  }*/

  test("dockWithML should generate the poses in expected format") {
    val molsWithSignAndScore = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("100mols.sdf").getPath)
      .generateSignatures
      .dockWithML(        
        dsInitSize = 20,
        numIterations = 20)
      .getMolecules
      .collect()

    val format = TestUtils.getFormat(molsWithSignAndScore(0))

    assert(format === ("AtomContainer", "String", "double"))

  }

  override def afterAll() {
    sc.stop()
  }

}