package se.uu.farmbio.vs

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.lang.Long
import scala.io.Source
import se.uu.farmbio.sg.types.Sig2ID_Mapping
import org.apache.spark.mllib.linalg.Vector
import java.io.PrintWriter
import org.apache.spark.mllib.regression.LabeledPoint


trait SGUtils_SerialTrait {

}

private[vs] object SGUtils_Serial {

  //Saving Sig2IdMap
  def saveSig2IdMap(sig2IdPath: String, sig2IdMap: Array[Sig2ID_Mapping]) = {
    val res = sig2IdMap.map { case (sig: String, id: scala.Long) => "" + id + "\t" + sig }
    val pw = new PrintWriter(sig2IdPath)
    res.foreach(pw.println(_))
    pw.close
  }
  
  //Loading Sig2IdMap
  def loadSig2IdMap(sig2IdPath: String): Array[Sig2ID_Mapping] = {
    val lines = Source.fromFile(sig2IdPath).getLines.toArray
    val sig2ID = lines.map {
      line =>
        val split = line.split('\t')
        (split(1), Long.valueOf(split(0)))
    }.asInstanceOf[Array[Sig2ID_Mapping]]
    sig2ID
  }
 
  //This is what we need or SGUtils.atoms2LP_carryData (non spark take array rather than RDDs)
  def generateNewSignatures(sdfFile: File, oldSig2IdMap : Array[Sig2ID_Mapping], h_start: Int, h_stop: Int)  = {
  
    //Returns Array[(mol: String, lps: LabeledPoint)]
  
  }
  
  
}

private[vs] class SGUtils_Serial() extends SGUtils_SerialTrait {

}