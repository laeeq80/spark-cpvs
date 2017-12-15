package se.uu.farmbio.vs

import java.io.File
import java.io.PrintWriter
import java.lang.Long

import scala.io.Source

import org.apache.spark.mllib.linalg.{Vector, Vectors}

import se.uu.farmbio.sg.types.Sig2ID_Mapping

import scala.reflect.ClassTag
import scala.collection.immutable.ListMap
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.signature.AtomSignature
import se.uu.farmbio.sg.exceptions._
import util.control.Breaks._
import scala.collection.JavaConversions._

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

	def atoms2LP_carryData[T: ClassTag](mols: Array[(T, IAtomContainer)],
			signatureUniverse: Map[String, Long],
			h_start: Int, 
			h_stop: Int): Array[(T, Vector)] = {

					mols.map{case((data: T, mol: IAtomContainer)) =>
					(data, atom2LP(mol, signatureUniverse, h_start, h_stop))};
	}

	def atom2LP(molecule: IAtomContainer, // The molecule to create signatures of
			signatureUniverse: Map[String, Long], // Signature-> "Feature ID"
			h_start: Int,
			h_stop: Int): Vector = {
					try {

						// Map is [Feature ID, #Occurrences]
						var feature_map = Map.empty[Long, Int];
						val h_stop_new = Math.min(molecule.getAtomCount - 1, h_stop); //In case a too big h_stop is set

						for (atom <- molecule.atoms()) {   //ERROR : value foreach is not a member of Iterable[org.openscience.cdk.interfaces.IAtom]
							for (height <- h_start to h_stop_new) {
								breakable{

									val atomSign = new AtomSignature(molecule.getAtomNumber(atom), height, molecule);
									val canonicalSign = atomSign.toCanonicalString();
									val signature_id: Long = signatureUniverse.getOrElse(canonicalSign, -1);
									if (signature_id == -1)
										break; // if not part of training model - skip signature // break the "breakable" - same as continue

									// Check if that signature has been found before for this molecule, update the quantity in such case
									val quantity = feature_map.getOrElse(signature_id, -1);  //ERROR : type mismatch; found : Any required: Long

									if (quantity == -1) {
										feature_map += (signature_id -> 1);  //ERROR : type mismatch; found : (Any, Int) required: (Long, Int)
									} else {
										feature_map += (signature_id -> (quantity + 1));
									}
								}
							}
						}
						// Convert feature map into (sparse) Vector
						val sortedFeatures = ListMap(feature_map.toSeq.sortBy(_._1):_*);
						var vectorIds: Array[Int] = Array.empty[Int];
						var vectorOccurrences: Array[Double] = Array.empty[Double];
						for (feature_id <- sortedFeatures.keys) {
							vectorIds = vectorIds :+ feature_id.toInt;   //ERROR : type mismatch; found : Long required: scala.collection.GenTraversableOnce[?]
							vectorOccurrences = vectorOccurrences :+ feature_map.get(feature_id).get.toDouble;
						}
						return Vectors.sparse(vectorIds.length, vectorIds, vectorOccurrences);  // ERROR : type mismatch; found : Array[Long] required: Array[Int] for vectorIds
						//ERROR : type mismatch; found : Array[Int] required: Array[Double] for vectorOccurrences

					} 
					catch {
					case ex : Throwable => throw new SignatureGenException("Unknown exception occured (in 'atom2SigRecord'), exception was: " + ex);
					}
	}

}

private[vs] class SGUtils_Serial() extends SGUtils_SerialTrait {

}