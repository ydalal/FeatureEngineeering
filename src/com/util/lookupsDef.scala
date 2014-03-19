package com.util

import java.util.ArrayList

object lookupsDef {
  // Validate the single column lookup files with return full record.
var dir: String = null;
  var keyDelim: String = null
  var areaCodeZipLookup: (Integer, String, String) => ArrayList[ArrayList[String]] = null //Output =  [city,state]
  var acZipByZipLookup: (Integer, String) => ArrayList[ArrayList[String]] = null //Output = [areacode,city,state]
  var blackListedPhoneLookup: (String) => ArrayList[String] = null
  var numUsersPerPhone: (String) => Int = null
  var acPrefixLookup: (String, String, String) => ArrayList[String] = null
  var postalRiskLookup: (Int, String) => Int = null
  var zipCountryLookup: (String) => Int = null
  var areaCodeGeoLookup: (Int, String) => ArrayList[String] = null
  var validCityLookup: (Int, String) => Boolean = null
  var userMsg = "1. Please make sure you providing correct field delimiters. 2. Please make sure they user keys are provided in increasing order while gettig value..";
  var debug = false; 
  var debug1 = false;
  var enableReuse = true; //True, allows reusing previously used lookup serialized instance from disk. False, reloads lookup from lookup file and creates a disk based representation.
  var write = true;
  dir = "/Users/ydalal/brgLookups";
  keyDelim = "#"
    
   /**
   * This method uses map type 1, which allows faster storing and retrieving of large lookup files.
   * Fields ::
   * day_phone;
   * num_users_per_phone;
   */
  def defNumUsersPerPhone(desc: String, fName: String): (String) => Int = {
    var delims = Array(" ", "\n");
    var keyIds: Array[Integer] = Array(1);
    var keyTypes = Array("Integer");
    val lkp = new LookupMapDB(desc, dir, fName, delims, keyIds, keyDelim, debug, false);

    /**
     * Inputs .. day_phone
     * Outputs .. num_users_per_phone
     *
     */
    def lookup(key1: String): Int = {
      var key = Array(key1);
      var res = lkp.getValue(key);
      if (res != null)
        try {
          res.get(0).get(0).toInt
        } catch { case e: Exception => if (debug1) println("\tWarning:NumUsersPerPhone Lookup 	throwed error for Value = " + key1 + "\n\t\tReturned 0"); 0 }
      else
        0
    }
    lookup
  }


    /**
   *    decimal(" ") cntry_id;
   * string(" ") postalcode;
   * decimal("\n") num_susp;
   */
  def defpostRiskLookup(desc: String, fName: String): (Int, String) => Int = {

    var delims = Array(" ", " ", "\n");
    var keyIds: Array[Integer] = Array(1, 2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);

    def lookup(key1: Int, key2: String): Int = {
      var key = Array(key1 + "", key2);
      var res = lkp.getValue(key);
      if (res != null)
        try {
          res.get(0).get(0).toInt
        } catch {
          case e: Exception =>
            if (debug1) println("\tWarning: PostalRiskLookup throwed error for Value = " + key1 + "\t" + key2 + "\n\t\tReturned 0");
            0
        }
      else
        0
    }
    lookup
  }
  
  
  /**
   * decimal("|") cntry_id;
   * string("\n") zip;
   */
  def defZipCountryLookup(desc: String, fName: String): (String) => Int = {
    var delims = Array("|", "|");
    var keyIds: Array[Integer] = Array(2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    def lookup(key1: String): Int = {
      var key = Array(key1);
      var res = lkp.getValue(key);
      if (res != null)
        try { res.get(0).get(0).toInt }
        catch {
          case e: Exception =>
            if (debug1) println("\tWarning:ZipCountryLookup throwed error for Value = " + key1 + "\n\t\tReturned -1");
            -1
        }
      else
        -1
    }
    lookup
  }



  /**
   * Fields
   * string(" ") area_code;
   * string(" ") status;
   * string(" ") state;
   * string("\n") prefix;
   */
  def defAcPrefixLookup(desc: String, fName: String): (String, String, String) => ArrayList[String] = {
    var delims = Array(" ", " ", " ", "\n");
    var keyIds: Array[Integer] = Array(3, 1, 4); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    def lookup(key1: String, key2: String, key3: String): ArrayList[String] = {
      var key = Array(key1, key2, key3); //Order Matter Here.
      var res = lkp.getValue(key);
      if (res != null)
        try {
          res.get(0)
        } catch {
          case e: Exception =>
            if (debug1) println("\tWarning: AcPrefixLookup throwed error for Value = " + key1 + "\t" + key2 + "\t" + key3 + "\n\t\tReturned null.");
            null
        }
      else
        null
    }
    lookup
  }

 
  /**
   * Fields ::
   * blacklist_phone;
   * toBeRemoved;
   *
   */
  def defblackListedPhoneLookup(desc: String, fName: String): (String) => ArrayList[String] = {
    var delims = Array(",", "\n");
    var keyIds: Array[Integer] = Array(1);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    /**
     * Input .. blaclist_phone
     * Output .. toBeRemoved ( 0 / 1 string )
     */
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getValue(key);
      if (res != null)
        try { res.get(0) } catch {
          case e: Exception =>
            if (debug1) println("\tWarning: BlackListedPhone Lookup   throwed error for Value = " + key1 + "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }

  /**
   * Fields ::
   *    	  decimal(" ") country; 1
   * string(" ") area_code; 2
   * string(" ") city;  3
   * string(" ") zip;  4
   * string("\n") state; 5
   */
  def defacZipByZipLookup(desc: String, fName: String): (Integer, String) => ArrayList[ArrayList[String]] = {

    var delims = Array(" ", " ", " ", " ", "\n");
    var keyIds: Array[Integer] = Array(1, 4);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);

    /**
     * Inputs .. country, zip
     * Output ..  List < area_code, city, state >
     */
    def lookup(country: Integer, zip: String): ArrayList[ArrayList[String]] = {
      var key = Array(country + "", zip); //convert key to strings.
      var res = lkp.getValue(key);
      if (res != null && res.size() > 0)
        res
      else
        null
    }
    lookup
  }

  /**
   * Fields ::
   * decimal(" ") country;
   * string(" ") area_code;
   * string(" ") city;
   * string(" ") zip;
   * string("\n") state;
   */
  def defAreaCodeLookup(desc: String, fName: String): (Integer, String, String) => ArrayList[ArrayList[String]] = {
    var delims = Array(" ", " ", " ", " ", "\n");
    var keyIds: Array[Integer] = Array(1, 2, 4);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    /**
     * Inputs .. area_code, city,state
     * Output .. zip
     */
    def lookup(country: Integer, area_code: String, zip: String): ArrayList[ArrayList[String]] = {
      var key = Array(country + "", area_code, zip); //convert key to strings.
      var res = lkp.getValue(key);
      if (res != null)
        res
      else
        null
    }
    lookup
  }

  /**
   * Method to print an Arraylist of string types, fields separated by space.
   */
  def printRecordList(in: ArrayList[String]) = {
    var i = 0
    while (i < in.size()) {
      print(in.get(i) + " ")
      i += 1;
    }
  }

  /**
   * Method to print an arraylist of arraylist of string type.
   */
  def printRecord(in: ArrayList[ArrayList[String]]) = {
    var i = 0
    while (i < in.size()) {
      var temp = in.get(i)
      var j = 0
      while (j < temp.size()) {
        print(temp.get(j) + " ")
        j += 1
      }
      i += 1
      println("")
    }
  }
  
  
   /**
   * Area Code Geo Lookup
   * Fields ::
   *  decimal("|") cntry_id;
   *  string("|") areacode;
   *  decimal("|") latitude;
   *  decimal("|") longitude;
   *  decimal("\n") phone_type;
   */
  def defAreaCodeGeoLookup(desc: String, fName: String): (Int, String) => ArrayList[String] = {
    var delims = Array("|", "|", "|", "|", "\n");
    var keyIds: Array[Integer] = Array(1, 2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    /**
     * Inputs .. cntry_id,area_code
     * Outputs .. latitude, longitude, phone_type
     */
    def lookup(key1: Int, key2: String): ArrayList[String] = {
      var key = Array(key1 + "", key2);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug1) println("\tWarning:AreaCodeGeoLookup throwed error for Value = " + key1 + "\t" + key2 + "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }
  
  
  /**
   * Valid City Lookup
   * Fields ::
   * 	decimal(" ") user_cntry_id;
   * 	utf8 string("\n") city;
   */
  def defvalidCityLookup(desc: String, fName: String): (Int, String) => Boolean = {
    var delims = Array(" ", "\n");
    var keyIds: Array[Integer] = Array(1, 2); //Key order matters
    var lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    /**
     * Inputs .. user_cntry_id, city
     * Output .. True/ False
     */
    def lookup(key1: Int, key2: String): Boolean = {
      var key = Array(key1 + "", key2);
      var res = lkp.getValue(key);
      if (res != null && res.size() >= 1)
        true
      else
        false
    }
    lookup
  }
  
   
  /**
   * Method to print array string each field separated by a space.
   */
  def printRecord(in: Array[String]) = {
    for (s <- in) {
      print(s + " ");
    }
  }
  
  
  //####################################### Email and IP Variables Lookups ######################################
  /**
  utf8 string("\t") ip4;
  decimal("\n")  ip4RR;
*/
  def defip4rr(desc:String,fName:String):(String)=>ArrayList[String]={
	var delims = Array("\t","\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:ip4rr lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }
  
  /**
  string("|") ip3;
  decimal("|") ip3_risk_prob;
  decimal("|")  ip3_risk_rating;
  decimal("\n") ip3_ato_risk;
   */
  def defip3rr(desc:String,fName:String):(String)=>ArrayList[String]={
	var delims = Array("|", "|", "|","\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:ip3rr lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }
  
  /**
  decimal(" ") cntry_id;
  decimal(" ") site_id;
  string("\n") site_name;
   */
  def defcountryMatchSite(desc:String, fName:String):(Integer)=>ArrayList[String]={
	 var delims = Array(" ", " ", "\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, false);
   
    def lookup(key1: Integer): ArrayList[String] = {
      var key = Array(key1+"");
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (true) println("\tWarning:CountryMatchSite lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }
  
  
  /** 
 string("|") ip4;
 string("|") ip3;
 decimal("|") dailyvol;
 string("|") min_octet4;
 string("|") max_octet4;
 decimal("|") level;
 decimal("|") levelname;
 decimal("\n") usercount;
 end; 
   */
   def defbadIp(desc:String,fName:String):(String)=>ArrayList[String]={
	var delims = Array("|", "|", "|","|", "|", "|","|","\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:badIp lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
  }
  
   /**
 string("|") domain;
 decimal("|") cnt_tot;
 decimal("|") cnt_bd;
 decimal("|") cnt_gd;
 decimal("|") score;
 decimal("|") totaltko;
 decimal("\n") rating2;
 end;
    */
   def defdomainscore(desc:String,fName:String):(String)=>ArrayList[String]={
     var delims = Array("|", "|", "|","|", "|", "|","\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:domainScore lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
   }

/**
decimal("|")latitude;
 decimal("|")longitude;
 string("|")ip3;
 string("|")aol_yn;
 string("|")dset;
 decimal("|")user_cntry_id;
 decimal("|")countryscore;
 string("|")state;
 decimal("|")statescore;
 decimal("|")ip3_lat;
 decimal("|")ip3_long;
 decimal("\n")zipscore;
*/
   def defmasterIpGeo(desc:String,fName:String):(String)=>ArrayList[String]={
     var delims = Array("|", "|", "|","|", "|", "|","|", "|", "|","|", "|", "\n");
    var keyIds: Array[Integer] = Array(3); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:masterIpGeo lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
   }
   
   /**
  decimal("|") ip4_int;
  string("|") ip4;
  string("\n") status;
    */
 def defIpProxy(desc:String,fName:String):(String)=>ArrayList[String]={
     var delims = Array("|", "|","\n");
    var keyIds: Array[Integer] = Array(2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:IpProxy lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
   }
   
 

 /**
  string("\n") freedomains;
 */
 def defFreeDomains(desc:String,fName:String):(String)=>ArrayList[String]={
    var delims = Array("\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:FreeDomain lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
   }

 /**
  * Returns country id for country domain
   decimal(" ") cntry_id;
  string("\n") cntry_domain;
 */
 def defCountryDomain(desc:String,fName:String):(String)=>ArrayList[String]={
    var delims = Array(" ","\n");
    var keyIds: Array[Integer] = Array(2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, false);
    def lookup(key1: String): ArrayList[String] = {
      var key = Array(key1);
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        try { res.get(0) }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:CountryDomain lookup throwed error for Value = " + key1+ "\n\t\tReturned null");
            null
        }
      else
        null
    }
    lookup
   }
   
 
 //TODO : Document
  def deftimeofdaylookup(desc:String,fName:String):(Int,Int)=>Double={
//  user_cntry_id, hour_of_day, hrpct
    var delims = Array( " ", " ","\n" );
	var keyIds:Array[Integer] = Array(1, 2);
	val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, false);
	
    def lookup(user_cntry_id:Int,datetime_hour:Int): Double = {
      var key = Array(user_cntry_id+"",datetime_hour+"");
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null )
        try { res.get(0).get(0).toDouble }
        catch {
          case e: Exception =>
            if (debug) println("\tWarning:timeofday lookup throwed error for Value = " + user_cntry_id+"\t\t"+datetime_hour+ "\n\t\tReturned 0.0");
            0.0
        }
      else
        0.0
    }
    lookup    
  }
  
  //TODO : Document
  def defdayofweeklookup(desc:String,fName:String):(Integer,String)=>Double={
	  //decimal(" ") user_cntry_id; string(" ") day_of_week;decimal("\n") daypct;
	  var delims = Array( " ", " ","\n" );
	  var keyIds:Array[Integer] = Array(1, 2);
	  val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, false);
	  def lookup(user_cntry_id:Integer,day_of_week:String):Double={
		  	var test = Array(user_cntry_id+"",day_of_week); //convert keys to strings.
		  	var res = lkp.getFullRecord(test,keyIds);
		  	if (res != null )
		  		try{ res.get(0).get(0).toDouble } catch{ case _: Exception => 0.0}
		  	else
		  	  0.0
	  }
	  lookup
  }
  
 /**
 string(",") lang;
 decimal(",") country_id;
 decimal("\n") risk;
 */
  def  defLangRisklookup(desc:String,fName:String):(String,Int)=>Double={
	  var delims = Array( ",",",","\n" );
	  var keyIds:Array[Integer] = Array(1,2);
	  val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, true, true);
	  
	  def lookup(lang:String,country_id:Int):Double={
		  	var test = Array(lang,country_id+""); //convert keys to strings.
		  	var res = lkp.getFullRecord(test,keyIds);
		  	if (res != null && res.get(0) !=null)
		  		try{res.get(0).get(2).toDouble} catch{ case _:Exception=> 0.0}
		  	else
		  	  0.0
	  }
	  lookup
  }
  
  
def  defAcceptLanguage(desc:String,fName:String):(String)=>ArrayList[String]={
//    string("^") id;
//  decimal("^") header_type;
//  string("^") language;
//  string("\n") time;
	  var delims = Array( "^","^","^","\n" );
	  var keyIds:Array[Integer] = Array(1);
	  val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, true, true);	  
	  def lookup(id:String):ArrayList[String]={
		  	var test = Array(id); //convert keys to strings.
		  	var res = lkp.getFullRecord(test,keyIds);
		  	if (res != null )
		  		try{res.get(0)} catch{ case _:Exception=> null }
		  	else
		  	  null
	  }
	  lookup  
  }

 /**
  * decimal("^") seven_digit_ph;
  * decimal("\n") ph_risk_rate;
  */
def defPRRLookup(desc:String,fName:String):(String)=>ArrayList[String]={
	var delims = Array( "^","\n" );
	var keyIds:Array[Integer] = Array(1);
	  val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, false);	  
	  def lookup(id:String):ArrayList[String]={
		  	var test = Array(id); //convert keys to strings.
		  	var res = lkp.getFullRecord(test,keyIds);
		  	if (res != null )
		  		try{res.get(0)} catch{ case _:Exception=> null }
		  	else
		  	  null
	  }
	  lookup
		
}

  /**
   * decimal("|") cntry_id;
   * string("|") zip;
   * string("|") zip_type;
   * string("|") state;
   * string("|") city;
   * string("|") area_code;
   * decimal("|") latitude;
   * decimal("\n") longitude;
   */
  def defzipGeoLookup(desc: String, fName: String): (Int, String) => ArrayList[ArrayList[String]] = {

    var delims = Array("|", "|", "|", "|", "|", "|", "|", "\n");
    var keyIds: Array[Integer] = Array(1, 2); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, true, true);
    def lookup(key1: Int, key2: String): ArrayList[ArrayList[String]] = {
      var test = Array(key1 + "", key2);
      var res = lkp.getFullRecord(test, keyIds);
      if (res != null)
        res
      else
        null
    }
    lookup
  }

  /**
   * decimal ("\t") country_id;
   * string("\t") country;
   * decimal ("\t") latitude;
   * decimal ("\t") longitude;
   * decimal ("\n") area;
   */
  def defcntryLatLongArea(desc: String, fName: String): (Int) => ArrayList[ArrayList[String]] = {
    var delims = Array("\t", "\t", "\t", "\t", "\n");
    var keyIds: Array[Integer] = Array(1); //order matters
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, false, true);
    def lookup(key1: Int): ArrayList[ArrayList[String]] = {
      var test = Array(key1 + "");
      var res = lkp.getFullRecord(test, keyIds);
      if (res != null)
        res
      else
        null
    }
    lookup
  }
  
  
   /**
   * type quovaipgeo_type = record
  string("|") ip3;
  string("|") start_ip;
  string("|") end_ip;  
  string("|") de_country;
  string("|") de_region;  
  string("|") de_city;
  string("|") de_connection_speed;  
  string("|") de_country_cf;
  string("|") de_region_cf;  
  string("|") de_city_cf;
  string("|") de_metro_code;  
  string("|") latitude;
  string("|") longitude;  
  string("|") de_country_code;
  string("|") de_region_code;  
  string("|") de_city_code;
  string("\n") de_continent_code;    
  end;
   */
  def deflookup_quovaipgeoLookup(desc: String, fName: String): (String, String) => ArrayList[ArrayList[String]] = {
    var delims = Array("|","|","|","|","|","|","|","|","|","|","|","|","|","|","|","|","\n");
    var keyIds: Array[Integer] = Array(1);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, true, true);
    
    def lookup(ip3: String,ip4: String): ArrayList[ArrayList[String]] = {
      var key = Array(ip3); //convert key to strings.
      var res = lkp.getFullRecord(key,keyIds);
      if (res != null)
        res
      else
        null
    }
    lookup
  }
  
  /**
   * Maps user_cntry_id and user_site_id to cross country risk score.
   * Fields ::
   * integer("\t") user_cntry_id;
   * string("\t") user_site_id;
   * decimal("\n") score;
   */
  def defcrossCntryRiskLookup(desc: String, fName: String): (Integer, Integer) => Double = {
    var delims = Array("\t", "\t", "\r\n");
    var keyIds: Array[Integer] = Array(1, 2);
    val lkp = new LookupMapDB2(desc, dir, fName, delims, keyIds, keyDelim, debug, false);
    /**
     * Inputs .. user_cntry_id, user_site_id
     * Output .. cross country risk score
     */
    def lookup(user_cntry_id: Integer, user_site_id: Integer): Double = {
      var key = Array(user_cntry_id + "", user_site_id + ""); //convert key to strings.
      var res = lkp.getValue(key);
      if (res != null)
        try {
          res.get(0).get(0).toDouble
        } catch {
          case e: Exception =>
            if (debug)
              println("Warning CrossCountryRiskScore throwed an exception." + e.printStackTrace() + "\n\t Returning 0.0 for user_cntry_id=" + user_cntry_id + " \t user_site_id= " + user_site_id);
            0.0
        }
      else
        0.0
    }
    lookup
  }
}




