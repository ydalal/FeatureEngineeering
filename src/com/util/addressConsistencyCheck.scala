package com.util

import java.io.File
import java.io.FileWriter
import java.util.ArrayList
import java.util.Scanner

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream

/*Note:
 * Please Use Desc in map creation if you pipeline uses same lookup for different purposes. Otherwise you can leave it empty.
 * Fast and Scalable Lookups Used.
 * In case of Errors
 * 1. Make sure you have used correct field delimiters. 2. Order of keys in lookup file and the getValue request are same.
 * KeyDelim String is not part of any of the key values.
 */
object addressConsistencyCheck {
  def initLookups() {
    numUsersPerPhone = defNumUsersPerPhone("NumUsersPerPhone Lookup", "xad.dat") //If we use mapdb2, it would be really slow.
    areaCodeZipLookup = defAreaCodeLookup("AreaCodeZip Lookup", "areacode_zip.dat")
    acZipByZipLookup = defacZipByZipLookup("AcZipByZip Lookup", "areacode_zip.dat")
    blackListedPhoneLookup = defblackListedPhoneLookup("BlackListedPhone Lookup", "RFC_PHONE_EXCLUSION.dat")
    postalRiskLookup = defpostRiskLookup("PostalRisk Lookup", "PostalRisk.dat")
    zipCountryLookup = defZipCountryLookup("ZipCntry Lookup", "zip_cntry_intl.dat")
    validCityLookup = defvalidCityLookup("ValidCity Lookup", "cityvalid.dat")
    acPrefixLookup = defAcPrefixLookup("AreaCodePrefix Lookup", "AreaCodePrefix.dat")
    areaCodeGeoLookup = defAreaCodeGeoLookup("AreaCodeGeo Lookup", "areacode_intl.dat")
  }

  /**
   * Method to validate lookup functionalities
   */
  def testLookups() = {
    println(numUsersPerPhone.apply("8187109707"))
    printRecord(areaCodeZipLookup.apply(1, "201", "07002")) //    1 201 BAYONNE 07002 NJ    
    printRecord(acZipByZipLookup.apply(1, "07002")) //TODO Need to get distinct record values associated with given set of keys.    
    val res = blackListedPhoneLookup.apply("sameasabove")
    if (res != null) {
      if (res.size() != 0) { println("Blacklisted") } else { println("Not BlackListed") }
    } else { println("Not BlacListed") }
    println(postalRiskLookup.apply(1, "90012")) //    1 90012 2 	
    println(zipCountryLookup.apply("00605")) //	  1|00501
    println(validCityLookup.apply(3, "ABERAERON")) //special case, working now yay/    	
    printRecordList(acPrefixLookup.apply("907", "AK", "209")) //    907 AS AK 209   
    printRecordList(areaCodeGeoLookup.apply(1, "201")) // issue, the output is coming in wrong order.//    1|254|31.62353333333333333333333333333333333333|-97.50646515151515151515151515151515151515|REGULAR
  }

  val write=false;
  val debug1=false;
  
  def main(args: Array[String]) {
    initLookups();
    //  testLookups();
    var path = "/Users/ydalal/BuyerRegScalaETL"
    var fileWrite=new File(path+File.separator+"address_consistencyOut_TabScala.txt");
    if(write){       if(fileWrite.exists()) fileWrite.delete();     }
    var fw = new FileWriter(fileWrite);
    var br = getFileReader(path+File.separator+"address_consistencyIn_Tab.txt")
     var str: String = null
    var count = 0; var goodRecCnt = 0;
    var recordId = 0;
    var colLen = 0
    var flag = true
    while (flag) {
      str = br.readLine
      if (str == null)
        flag = false
      else {
        recordId += 1
        try {
          var split = str.split("\t")
          if (split.length != 48) {
            count += 1;
          } else {
            /*
			12 Colmns Set
			addr, blng_curncy_id, city, dayphone, email, gender_mfu, last_modfd, nightphone, pref_categ_interest1_id, pref_categ_interest2_id, pstl_code, state 
			user_cntry_id, user_flags, user_id, user_name, user_regn_id, user_site_id, user_slctd_id, uvdetail, uvrating, user_confirm_code, user_sex_flag  
			user_ip_addr, req_email_count, payment_type, date_confirm, nbr_stored_address, flagsex2, date_of_birth, aol_master_id, tax_id, linked_paypal_acct, paypal_link_state, flagsex3
			cre_date, user_cre_date, flagsex4, flagsex5, flagsex6, weight equifax_sts, upd_date, addr1, addr2, areacode, prefix, row_id
			*/
            //printRecord(split)
            var user_id = split.apply(14).toLong
            var user_cntry_id = split.apply(12).toInt
            var area_code: String = split.apply(45)
            var pstl_code = split.apply(10)
            var dayphone = split.apply(3)
            var state = split.apply(11)
            var city = split.apply(2)
            var prefix = split.apply(46)
//            if (debug1)
            if (user_id==1194366149)
              println(user_cntry_id + "\t\t" + area_code + "\t\t" + pstl_code + "\t\t" + dayphone + "\t\t" + state + "\t\t" + city + "\t\t" + prefix)
            transform(fw,user_id, user_cntry_id, area_code, pstl_code, dayphone, state, city, prefix);
            goodRecCnt += 1
//            if (goodRecCnt == 100)
//              flag = false
          }
        } catch {
          case e: Exception => if (debug1) println("Warning:Dropping Record Id : " + recordId);
        }
      }
    }
    println("\n\nRecords Count where column width is not 47, is = " + count + "\t Good records Count = " + goodRecCnt)
  	fw.close();
  }

  /**
   * Address Consistency Variable Transformation Functions.
   * This functions takes raw fields as  input and uses lookups and other data transformation techniques to generate the final vector.
   */
  def transform(fw:FileWriter,user_id: Long, user_cntry_id: Int, area_code: String, pstl_code: String, dayphone: String, state: String, city: String, prefix: String) = {
    val ac_zip_info: ArrayList[ArrayList[String]] = areaCodeZipLookup(user_cntry_id, area_code, string_replace(string_upcase(pstl_code), " ", "")) //[city,state]
    val zip_info: ArrayList[ArrayList[String]] = acZipByZipLookup(user_cntry_id, string_replace(string_upcase(pstl_code), " ", "")) //[area_code,city,state]
    val blackListPhone = string_filter(dayphone, "0123456789") //
    val bkphone: ArrayList[String] = blackListedPhoneLookup(blackListPhone) //
    val num_users_phone = numUsersPerPhone(string_filter(dayphone, "0123456789")) //[count:Int]
    val ac_prefix = acPrefixLookup(area_code, state, prefix);
    val postal_risk = postalRiskLookup(user_cntry_id, string_filter_out(string_upcase(pstl_code), " "));
    val zipcntry = zipCountryLookup(pstl_code); //return int ( cntry_id )
    //Validity of Address Fields With Lookups
    var valid_cntry_zip: String = null
    var valid_ac: String = null
    var valid_zip: String = null
    var valid_ac_zip: String = null
    var valid_ac_prefix: String = null
    var city_ac_zip: String = null
    var state_ac_zip: String = null
    var nbr_users_per_phone: Int = -1;
    var phone_black: String = null
    		
    var city_trim: String = null
    var city_trim_up: String = null
    if (zipcntry == -1)
      valid_cntry_zip = "9";
    else if (zipcntry == user_cntry_id)
      valid_cntry_zip = "1";
    else
      valid_cntry_zip = "0";
    //valid_zip
    if (user_cntry_id == 1 || user_cntry_id == 2)
      if (is_null(zip_info))
        valid_zip = "0"
      else
        valid_zip = "1"
    else
      valid_zip = "9"
    if (user_cntry_id == 1 || user_cntry_id == 2)
      if (is_null(areaCodeGeoLookup.apply(user_cntry_id, area_code)) && (area_code != "800") && (area_code != "888") && (area_code != "877") &&
        (area_code != "866") && (area_code != "855") && (area_code != "844") && (area_code != "833") && (area_code != "822"))
        valid_ac = "0";
      else
        valid_ac = "1";
    else
      valid_ac = "9";
    if (user_cntry_id == 1) {
      if (is_null(ac_prefix))
        valid_ac_prefix = "0";
      else
        valid_ac_prefix = "1";

    } else
      valid_ac_prefix = "9";

    if (user_cntry_id == 1 || user_cntry_id == 2) {
      if (is_null(ac_zip_info) || ac_zip_info.size() <= 0) { //ydalal added ac_zip_info condition here.
        valid_ac_zip = "0"; state_ac_zip = "9"; city_ac_zip = "9";
      } else {
        valid_ac_zip = "1";
        if (string_replace(string_upcase(state), " ", "") == string_upcase(ac_zip_info.get(0).get(1)))
          state_ac_zip = "1";
        else
          state_ac_zip = "0";
        if (string_replace(string_upcase(city), " ", "") == string_upcase(ac_zip_info.get(0).get(0)))
          city_ac_zip = "1";
        else
          city_ac_zip = "0";
      }
    } else {
      valid_ac_zip = "9";
      state_ac_zip = "9";
      city_ac_zip = "9";
    }
    if (is_null(bkphone))
      phone_black = "0";
    else if (string_lrtrim(blackListPhone) == "")
      phone_black = "9";
    else
      phone_black = "1";
    if (num_users_phone == 0)
      nbr_users_per_phone = 1;
    else
      nbr_users_per_phone = num_users_phone
    city_trim = string_lrtrim(city);
    city_trim_up = string_upcase(city_trim);
    var city_valid = if (!validCityLookup(user_cntry_id, city_trim_up)) "N" else "Y";
    var pstl_code_susp = if (is_null(postal_risk)) 0 else postal_risk;
    var zip_valid = if (is_null(zip_info)) "N" else "Y";
    if(write)
    {
      fw.write(user_id + "\t" + pstl_code_susp + "\t" + valid_cntry_zip + "\t" + zip_valid + "\t" + city_valid + "\t" + valid_ac_zip + "\t" + city_ac_zip + "\t" + state_ac_zip + "\t" + valid_ac + "\t" + valid_zip + "\t" + valid_ac_prefix + "\t" + nbr_users_per_phone + "\t" + phone_black)
      fw.write("\n");
    }
  }



}
