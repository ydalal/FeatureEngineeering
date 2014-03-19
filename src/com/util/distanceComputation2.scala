package com.util

import java.io.File
import java.util.ArrayList

import distancFunctions.compute_distance

object distanceComputation2 {
  var lookupDir: String = null;
  var lkpDelim: String = null
  var areaCodeGeoLookup: (Int, String) => ArrayList[String] = null
  var cntryLatLongArea: (Int) => ArrayList[ArrayList[String]] = null
  var zipGeoLookup: (Int, String) => ArrayList[ArrayList[String]] = null
  var masterIpGeoLookup: (String) => ArrayList[String] = null
  val debug = false
  val tab="\t"

    def initLookups() {
    areaCodeGeoLookup = defAreaCodeGeoLookup("AreaCodeGeo Lookup", "areacode_intl.dat")
    cntryLatLongArea = defcntryLatLongArea("CntryLatLongArea Lookup", "cntry_lat_long_area.txt")
    zipGeoLookup = defzipGeoLookup("ZipGeo Lookup", "zip_geo_intl.dat")
    masterIpGeoLookup = defmasterIpGeo("MasterIpGeo Lookup", "masteripgeo.dat")
  }
  
  def main(args: Array[String]) {
    initLookups()
    var path = "/Users/ydalal/" + File.separator + "address_consistencyIn_Tab.txt"
    var br = getFileReader(path);
    var run = true
    var count=0
    while (run) {
      val str = br.readLine()
      if (str == null) {
        run = false
      } else { //Read File Line by Line

        try {
          var split = str.split("\t")
          if (split.length == 48) {
            /*
			12 Colmns Set
			addr, blng_curncy_id, city, dayphone, email, gender_mfu, last_modfd, nightphone, pref_categ_interest1_id, pref_categ_interest2_id, pstl_code, state 
			user_cntry_id, user_flags, user_id, user_name, user_regn_id, user_site_id, user_slctd_id, uvdetail, uvrating, user_confirm_code, user_sex_flag, user_ip_addr,
			req_email_count, payment_type, date_confirm, nbr_stored_address, flagsex2, date_of_birth, aol_master_id, tax_id, linked_paypal_acct, paypal_link_state, flagsex3, cre_date,
			user_cre_date, flagsex4, flagsex5, flagsex6, weight equifax_sts, upd_date, addr1, addr2, areacode, prefix, row_id
			*/
            var user_id = split.apply(14).toLong
            var user_cntry_id = split.apply(12).toInt
            var area_code: String = split.apply(45)
            var pstl_code = split.apply(10)
            var user_ip_addr = split.apply(23)
            var state = split.apply(11)
            var city = split.apply(2)
            var prefix = split.apply(46)
//            println(user_id,tab,user_ip_addr,tab,prefix,tab,area_code,tab,user_cntry_id,tab,pstl_code,tab,state)
            transform(user_id,user_ip_addr, prefix, area_code, user_cntry_id, pstl_code, state)
            count += 1
            if(count ==100 ){
              run = false;
            }
          }
        } catch {
          case e: Exception => e.printStackTrace();
        }
      }
    }
    br.close();
  }

  def transform(user_id:Long,user_ip_addr: String, prefix: String, areacode: String, user_cntry_id: Int, pstl_code: String, state: String) {
    var ip3_lat = 0.0; /*get the lat/long for the given ip3. --> shouldnt this be initialized to south pole co-ordinates ?*/
    var ip3_long = 0.0;
    var quova_ip3_lat = 0.0; /*get the lat/long for the given ip3. --> shouldnt this be initialized to south pole co-ordinates ?*/
    var quova_ip3_long = 0.0;
    var pstl_lat = 0.0;
    var pstl_long = 0.0;
    var ac_lat = 0.0;
    var ac_long = 0.0;
    var ip3_zip_dist = 999.0;
    var ip3_ac_dist = 999.0;
    var quova_ip3_zip_dist = 999.0;
    var quova_ip3_ac_dist = 999.0;
    var cntry_lat = 0.0;
    var cntry_long = 0.0;
    var cntry_area = 0.0;
    var ac_zip_dist = 999.0;
    var ind: Int = string_rindex(user_ip_addr, ".");
    var ip3: String = string_lrtrim(string_substring(user_ip_addr, 1, ind - 1));

    //Going to take longer time.
    //This is a synthesized dataset from quova or digital envoy as well as ebay users self reporting
    //		  var ip3geo:masteripgeo_type = lookup_masteripgeo(ip3); 
    var ip3geo = masterIpGeoLookup.apply(ip3)
    var ip3_zip_dist_exists: String = null;
    var ip3_ac_dist_exists: String = null;
    var ac_zip_dist_exists: String = null;
    //this is original dataset from quova or digital envoy
    //let quovaipgeo_type quovaip3geo = lookup_quovaipgeo(ip3, string_lrtrim(in.user_ip_addr) );
    var temp_zip: String = null;
    // decimal("|") cntry_id;  	string("|") zip;  	string("|") zip_type;  	string("|") state;  	string("|") city;  	string("|") area_code;  	decimal("|") latitude;  	decimal("\n") longitude;
    var acgeo = areaCodeGeoLookup.apply(user_cntry_id, areacode);
    //decimal ("\t") country_id;string("\t") country;decimal ("\t") latitude;decimal ("\t") longitude;decimal ("\n") area;
    var cntrygeo = cntryLatLongArea.apply(user_cntry_id); //returns [country,latitude,longitude,area]
    var i: Int = 0;
    var done: Int = 0;
    if (is_null(acgeo) || acgeo.size() == 0) {
      var temp: String = areacode;
      for (i <- 0 until 5 if (done == 0)) {
        temp = string_concat(temp, string_substring(prefix, i + 1, 1));
        acgeo = areaCodeGeoLookup.apply(user_cntry_id, temp);
        if (!is_null(acgeo) && acgeo.size()!=0)//replaced following definition is_defined(acgeo.get(0)
          done = 1;
      }
    }
    ip3_lat = if (is_null(ip3geo) || ip3geo.size() == 0) 0.0 else ip3geo.get(9).toDouble //ip3_lat;
    ip3_long = if (is_null(ip3geo) || ip3geo.size() == 0) 0.0 else ip3geo.get(10).toDouble // ip3_long;
    cntry_lat = if (is_null(cntrygeo) || cntrygeo.size() == 0) 0.0 else cntrygeo.get(0).get(2).toDouble; //lat
    cntry_long = if (is_null(cntrygeo) || cntrygeo.size() == 0) 0.0 else cntrygeo.get(0).get(3).toDouble; //long
    cntry_area = if (is_null(cntrygeo) || cntrygeo.size() == 0) 0.0 else cntrygeo.get(0).get(4).toDouble; //area

    if (user_cntry_id != 3)
      temp_zip = string_filter_out(pstl_code, " ");
    else {
      var ind_space = 0;
      temp_zip = string_lrtrim(pstl_code);
      ind_space = string_index(temp_zip, " ");
      if (ind_space != 0)
        temp_zip = string_concat(string_substring(temp_zip, 1, ind_space - 1), string_substring(temp_zip, ind_space + 1, 1));
    }

    var zipgeo = zipGeoLookup.apply(user_cntry_id, string_upcase(temp_zip));
    
    pstl_lat = if (is_null(zipgeo) || zipgeo.size() == 0) 0.0 else zipgeo.get(0).get(6).toDouble; //lat
    pstl_long = if (is_null(zipgeo) || zipgeo.size() == 0) 0.0 else zipgeo.get(0).get(7).toDouble; //long
    
    ac_lat = if (is_null(acgeo) || acgeo.size() == 0) 0.0 else acgeo.get(2).toDouble; //lat
    ac_long = if (is_null(acgeo) || acgeo.size() == 0) 0.0 else acgeo.get(3).toDouble; //long

    if (is_null(zipgeo) || is_null(ip3geo) || tnsar_geo_location_supported(user_cntry_id) < 1) {
      ip3_zip_dist = 0;
      ip3_zip_dist_exists = "N";
    } else {
      ip3_zip_dist = compute_distance(ip3_lat, ip3_long, pstl_lat, pstl_long, "miles").toDouble; //TODO, test if toInt is working as expected.
      ip3_zip_dist_exists = "Y";
    }
    if (is_null(zipgeo) || is_null(acgeo) || tnsar_geo_location_supported(user_cntry_id) < 1) {
      ac_zip_dist = 0;
      ac_zip_dist_exists = "N";
    } else {
      ac_zip_dist = compute_distance(ac_lat, ac_long, pstl_lat, pstl_long, "miles").toDouble;
      ac_zip_dist_exists = "Y";
    }
    if (is_null(ip3geo) || is_null(acgeo) || tnsar_geo_location_supported(user_cntry_id) < 1) {
      ip3_ac_dist = 0;
      ip3_ac_dist_exists = "N";
    } else {
      ip3_ac_dist = compute_distance(ip3_lat, ip3_long, ac_lat, ac_long, "miles").toDouble;
      ip3_ac_dist_exists = "Y";
    }
    var dist_ip3_zip_lt_50 = if (ip3_zip_dist < 50) "Y" else "N";
    var dist_ip3_zip_bw_50_300 = if (ip3_zip_dist >= 50 && ip3_zip_dist <= 300) "Y" else "N";
    var dist_ip3_zip_gt_300 = if (ip3_zip_dist > 300) "Y" else "N";
    var dist_ac_zip = ac_zip_dist;
    var dist_ac_zip_lt_50 = if (ac_zip_dist < 50) "Y" else "N";
    var dist_ac_zip_bw_50_300 = if (ac_zip_dist >= 50 && ac_zip_dist <= 300) "Y" else "N";
    var dist_ac_zip_gt_300 = if (ac_zip_dist > 300) "Y" else "N";
    var dist_ip3_areacode = ip3_ac_dist;
    var dist_ip3_areacode_lt_50 = if (ip3_ac_dist < 50) "Y" else "N";
    var dist_ip3_areacode_bw_50_300 = if (ip3_ac_dist >= 50 && ip3_ac_dist <= 300) "Y" else "N";
    var dist_ip3_areacode_gt_300 = if (ip3_ac_dist > 300) "Y" else "N";
    var ip3_state_score = if (!is_null(ip3geo) && !ip3geo.get(8).isEmpty()) string_lrtrim(ip3geo.get(8)) else 0 //ip3geo.get(0).get(8) = statescore 
    var ip3_state_match = if (!is_null(ip3geo) && string_upcase(ip3geo.get(7)) == string_upcase(state)) "Y" else "N";
    var ip3_zip_score = if (!is_null(ip3geo) && string_rindex(user_ip_addr, ".") < 0) string_split(ip3geo.get(11), "|").apply(0) else 0;
    var phone_type = if (!is_null(acgeo)) acgeo.get(4) else "UNKNOWN";
    val dist_ip3_zip=ip3_zip_dist
    println(user_id+tab+dist_ip3_zip+tab+dist_ip3_areacode +tab+dist_ac_zip+tab+cntry_area+tab+dist_ip3_zip_lt_50+tab+dist_ip3_zip_bw_50_300+tab+dist_ip3_zip_gt_300+tab+dist_ip3_areacode_lt_50+tab+dist_ip3_areacode_bw_50_300+
        tab+dist_ip3_areacode_gt_300+tab+dist_ac_zip_lt_50+tab+dist_ac_zip_bw_50_300+tab+dist_ac_zip_gt_300+tab+ip3_state_score+tab+ip3_state_match+tab+ip3_zip_score+
        tab+phone_type+tab+"zipgeo"+tab+"acgeo"+tab+pstl_code+tab+user_cntry_id+tab+user_ip_addr+tab+"ip3geo"+tab+ip3_ac_dist_exists+tab+ac_zip_dist_exists+tab+ip3_zip_dist_exists)
  }
}