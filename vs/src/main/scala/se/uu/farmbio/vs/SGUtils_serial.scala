package se.uu.farmbio.vs

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.lang.Long
import scala.io.Source
import se.uu.farmbio.sg.types.Sig2ID_Mapping
import java.io.PrintWriter

trait SGUtils_SerialTrait {

}

private[vs] object SGUtils_Serial {

  def saveSig2IdMap(sig2IdPath: String, sig2IdMap: Array[Sig2ID_Mapping]) = {
    val res = sig2IdMap.map { case (sig: String, id: scala.Long) => "" + id + "\t" + sig }
    val pw = new PrintWriter(sig2IdPath)
    res.foreach(pw.println(_))
    pw.close
  }

  def loadSig2IdMap(sig2IdPath: String): Sig2ID_Mapping = {
    val lines = Source.fromFile(sig2IdPath).getLines.toArray
    val sig2ID = lines.map {
      line =>
        val split = line.split('\t')
        (split(1), Long.valueOf(split(0)))
    }.asInstanceOf[Sig2ID_Mapping]
    sig2ID
  }

  /* UNFINISHED
  /**
   * atoms2LP_UpdateSignMapCarryData performs signature generation on new molecules, but
   * in contrast to the non "UpdateSignMap" functions, it will add new signatures to the
   * "signature universe".
   */
  def atoms2LP_UpdateSignMapCarryData[T: ClassTag](molecules: Array[(T, Double, IAtomContainer)],
                                                   old_signMap: Array[(String, Long)],
                                                   h_start: Int,
                                                   h_stop: Int): (Array[(T, LabeledPoint)], Array[(String, Long)]) = {

    // Perform Signature Generation on all molecules 
    val array_sigRecordDecision = molecules.map {
      case ((data: T, label: Double, mol: IAtomContainer)) =>
        (data, (label, SGUtils.atom2SigRecord(mol, h_start, h_stop)))
    };

    //Pick out all signatures
    val allSignatures: Array[String] = array_sigRecordDecision.flatMap {
      case (_: T, (_: Double, theMap: Map[String, Int])) =>
        theMap.keySet
    }.distinct

    // Find the highest previous signatureID:
    val highestID =
      try {
        old_signMap.map { case (_: String, id: Long) => id }.max;
      } catch {
        case _: java.lang.UnsupportedOperationException => 0L // meaning no old signatures present
      }

    // Find the new Sig2ID_Mapping with both old and new signatures
    val old_sign = old_signMap.map { case (sign: String, _: Long) => sign }
    val newSignMap = allSignatures.diff(old_sign) 
    
    val new_signMap = allSignatures.subtract(old_signMap.map {
      case (sign: String, _: Long) => sign
    }).zipWithUniqueId
    .map { case (sign: String, old_id: Long) => (sign, old_id + highestID + 1) } ++ old_signMap;

    val d = new_signMap.map { case (_, id) => id }.max + 1;

    // Generate feature vectors with the new Sig2ID_Mapping
    val rdd_withFuture_vectors = getFeatureVectors_carryData(array_sigRecordDecision, new_signMap);

    (sig2LP_carryData(rdd_withFuture_vectors, d), new_signMap);

  }
  
  /**
   * atoms2LP_carryData take an Array of new molecules och computes signature generation.
   * This function only uses the Signatures given in the "signatureUniverse" parameter.
   * @param molecules   The molecules, their labels and carryData
   * @param signatureUniverse The mapping of signatures->id that will be used
   * @param h_start     The start of signature generation
   * @param h_stop      The stop of signature generation
   * @param sc          The SparkContext
   * @return            The LabeledPoint-records generated, with corresponding carryData
   */
  def atoms2LP_carryData[T: ClassTag](
      molecules: Array[(T, Double, IAtomContainer)], 
      signatureUniverse: Array[Sig2ID_Mapping],
      h_start: Int, 
      h_stop: Int): Array[(T,LabeledPoint)]={
    
    val array_sigRecordDecision = molecules.map{case((data: T, label: Double, mol: IAtomContainer))=>
      (data, (label, SGUtils.atom2SigRecord(mol, h_start, h_stop)))};
    //val rdd_withID = rdd_sigRecordDecision.zipWithUniqueId;
    val rdd_withFuture_vectors = getFeatureVectors_carryData(array_sigRecordDecision, signatureUniverse);
    
    //Find the biggest d (dimension)
    var d=0L;
    try{
      d = signatureUniverse.map { case((_, sigID)) => sigID }.max +1;
    }
    catch{
      case e: java.lang.UnsupportedOperationException => //This means that the Sign-mapping was empty!
    }
    
    sig2LP_carryData(rdd_withFuture_vectors, d);
  }

  /**
   * Take the array_input (your data-records) and the unique mapping of Signature->ID
   * Transform data-records to use the Signature->ID mapping. For molecules that has no
   * signatures in the signatureUniverise will be removed from the output!
   * @param array              The RDD containing the data using Signatures
   * @param uniqueSignatures The unique signatures (mapping Signature->ID)
   * @return    Array[SignatureRecordDecision_ID] (Value/Class, Map[Signature_ID, #occurrences])
   */
  private[vs] def getFeatureVectors_carryData[T: ClassTag](array: Array[(T,SignatureRecordDecision)],
      uniqueSignatures: Array[Sig2ID_Mapping]): Array[(T, SignatureRecordDecision_ID)] = {

    // Add a unique ID to each record: 
    val req_with_id: Array[((T, SignatureRecordDecision), Int)] = array.zipWithIndex
    // So here we store (T, (Double, Map[String, Int])) -> ID

    // Sig2ID_Mapping = (String, Long)
    //((Double, Map[String, Int]), Long) = (SignatureRecordDecision, Long)
    // transform to: (String, (Double, Int, Long)) format for joining them!

    // Expand the Maps 
    val expanded_Array = req_with_id.flatMap {
    //case((carry: T, (dec: Double, mapping: Map[String, Int])), molID: Long ) =>
      case((carry, (dec, mapping)), molID ) =>
        mapping.toSeq. //get the map as Seq((String,Int), Int)
          map {
            case (signature: String, count: Int) =>
              (signature, (count, molID))
          } // result: (signature, (#of occurrences of this signature, MoleculeID))
    }

    //: RDD[(String,((Double, Int, Long), Long))]
    // combine the unique_ID with expanded mapping
    val joined = expanded_Array.join(uniqueSignatures);
    // ((signature,height), ((#occurrences, MoleculeID), Signature_ID))

    // (MoleculeID, Signature_ID, #occurrences, value)
    val res_rdd: RDD[(Long, (Long, Int))] = joined.map {
      case (signature, ((count, mol_ID), sign_ID)) =>
        (mol_ID, (sign_ID, count));
      }

    // Group all records that belong to the same molecule to new row
    val result: RDD[(Long, Map[Long, Int])] = res_rdd.aggregateByKey(Map.empty[Long, Int])(
      seqOp = (resultType, input) => (resultType + (input._1 -> input._2)),
      combOp = (resultType1, resultType2) => (resultType1 ++ resultType2)
    );
        
    //(Double, Map[Long, Int]);
    // add the carry and decision/regression value
    req_with_id.map{//case ((carry: T, (dec: Double, _: Map[String, Int])), molID: Long)=>
      case ((carry, (dec, _)), molID)=>
        (molID, (carry, dec))}.
      join(result).
      map { //case(molID: Long, ((carry: T, dec: Double),(mapping: Map[Long, Int])))=>
        case(molID, ((carry, dec),(mapping)))=>
          (carry, (dec, mapping))
    }
  }*/

}

private[vs] class SGUtils_Serial() extends SGUtils_SerialTrait {

}