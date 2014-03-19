package com.util

/**
 * @author ydalal
 */
object crossCountryRisk {
  var crossCntryRiskLookup: (Integer, Integer) => Double = null
  
  def main(args: Array[String]) {
    println(crossCountryLkp(168,0))
    println(crossCountryLkp(1, 0))
    println(crossCountryLkp(53, 0))
    println(crossCountryLkp(96, 0))    
    println(crossCountryLkp(80, 0))
  }
  
  def crossCountryLkp(user_cntry_id: Integer, user_site_id: Integer) :Double={
    if(crossCntryRiskLookup == null ){
        crossCntryRiskLookup = defcrossCntryRiskLookup("CrossCountryRisk Lookup", "crossCountryRiskScore.dat")
    }
    crossCntryRiskLookup.apply(user_cntry_id, user_site_id)
  }
  
}
