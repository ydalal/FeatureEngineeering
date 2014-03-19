package com.util

import java.util.ArrayList

import com.google.i18n.phonenumbers.NumberParseException
import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File

/**
 * @author ydalal
 * This object is used to parse phone number for given country and output ac_prefix , areacode and prefix.
 */
object parsePhoneNumber {
  val phoneUtil: PhoneNumberUtil = PhoneNumberUtil.getInstance();
  var dir = ""
  var keyDelim = ""
  var debug=false
  
 def main(args: Array[String]) {
    dir = "C:\\Users\\ydalal\\scala_etl\\lookups";
    keyDelim = "#"
    var cntryIdtoIsoCodeLookup: (Integer) => String = null
    cntryIdtoIsoCodeLookup = defcntrytoisocodelkp("", "countryId2IsoCode.dat")
    var path = "/Users/ydalal/brgLookups"
    var reader = new InputStreamReader(new FileInputStream(path+File.separator+"address_consistencyIn_Tab.txt"), "UTF-8");
    var br = new BufferedReader(reader);
    var str: String = null
    var flag = true
    var goodCount=0;
    var totalCount=0;
    var excepCount=0;
    var excepCount2=0;
    while (flag) {
      str = br.readLine
      if (str == null)
        flag = false
      else {
        try {
          var split = str.split("\t")
          if (split.length != 48) {
          } else {
            totalCount+=1
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
            var dayphone = split.apply(3)
            var prefix = split.apply(46)
            var scala_area_code=""
            var scala_prefix=""
            try{
              var areacode_prefix=extract_areacode(dayphone, cntryIdtoIsoCodeLookup.apply(user_cntry_id));//Note Area code and prefix can be in different part of the parsed number string current code works for majority of country. But definition of prefix and areacode is subjective in several other countries.              
              if(areacode_prefix.length()>1) 	  scala_area_code=	areacode_prefix.split("#").apply(0)
           	  if(areacode_prefix.length()>1)       scala_prefix=	areacode_prefix.split("#").apply(1)
            }catch{
              case e: Exception => if (true) println("Warning:Dropping Record"+e.printStackTrace()); excepCount2+=1;
            }
          println(user_cntry_id + "\t|\t" + dayphone + "\t|\t"+area_code +"\t|\t"+scala_area_code+"\t|\t" +  prefix ++"\t|\t"+scala_prefix);
            goodCount+=1;
          }
        } catch {
          case e: Exception => if (true) println("Warning:Dropping Record "+e.printStackTrace()); excepCount+=1;
        }
      }
    }
    println("Stats : \nTotalCount= "+totalCount+" GoodRecordCount= "+goodCount+" ExcpetionInGoodrecords= "+excepCount+ " ExcpetionInGoodrecords2= "+excepCount2)
  }

  /**
   * Method to parse phone number and return areacode and prefix.
   * phnumber.. User's phone number.
   * user_cntry_code .. Iso country code for user's country.
   */
  def extract_areacode(phnumber: String, user_cntry_code: String): String = {
    var delim="#"
    var phNbr = phnumber.trim().replaceAll("[\\(\\)\\-\\*\\,\\.\\/\\:\\;\\_\\|\\+ ]", ""); ///trim and remove delimiters from phone number
    var output: String = "#";
    try {
      val phNbrObj: PhoneNumber = phoneUtil.parse(phNbr, user_cntry_code.toUpperCase());
      if (phoneUtil.isValidNumber(phNbrObj)) {
        val intlFormat = phoneUtil.format(phNbrObj, PhoneNumberFormat.INTERNATIONAL)
        val res = intlFormat.split("[- ]") //split on basis of spaces,-	    
        if(res!=null && res.length > 1)
        	output = res.apply(1) + delim + res.apply(2)
      }
    } catch { //if phone is not valid return empty results.
      	case e: Exception => if(debug) println("Warning PhoneParse throwed Exception : " + e.toString()+"\n\tPhoneNumber="+phnumber+" UserCountryCode= "+user_cntry_code);
    }
    output
  }

  /**
   * Map country id to iso country code
   * Fields ::
   * integer("\t") cntry_id;
   * string("\n") iso_cntry_code;
   */
  def defcntrytoisocodelkp(desc: String, fName: String): (Integer) => String = {
    //  Lookup to map user_cntry_id to iso country code. 
    var delims = Array("\t", "\r\n");
    var keyIds: Array[Integer] = Array(1);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    /**
     * Inputs .. cntry_id
     * Output .. iso_cntry_id
     */
    def lookup(cntry_id: Integer): String = {
      var key = Array(cntry_id + ""); //convert key to strings.
      var res = lkp.getValue(key);
      if (res != null)
        try {
          res.get(0).get(0)
        } catch {
          case e: Exception =>
            println("Warning CountryIdtoIsoCodeLkp throwed an exception. Returning \"\" for Value =" + cntry_id);
            ""
        }
      else
        ""
    }
    lookup
  }

  /**
   * Method to test various country's use cases.
   */
  def test() {
    //  Exceptional Use Case, Croatia has 385 + Mobile Code (95 ) + Number ( area_code+prefix)
    //  Exceptional Use Case 2, India Mobile phone also has Mobile Code(94/98) + area_code + prefix.
    val swissNumberStr: String = "044 668 18 00" //CH
    val usPhoneNumber: String = "918|754|2659".replaceAll("#", " ") //1,US
    val canadaPhoneNumber: String = "250|857|3287".replaceAll("#", "")
    val russianPhoneNumber: String = "9122698666" //168,RU
    val croatisPhoneNumber = "0955131512" //53,hr (Croatia, Democratic Republic of the)
    val indonesiaPhoneNumber = "081913139810" //, "ID"
    val malaysiaPhoneNumber = "0178611251" //, "MY"
    println(extract_areacode("017,8611.;;25_+)))(1", "MY")) //Malaysia Phone Number
    println(extract_areacode("0178611251", "MY")) //Malaysia Phone Number
    println(extract_areacode("0543319433", "il")) //Israel Phone Number
  }
}