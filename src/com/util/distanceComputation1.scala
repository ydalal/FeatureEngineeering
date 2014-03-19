package com.util

import java.util.ArrayList

object distanceComputation1 {
  var lookupDir: String = null;
  var lkpDelim: String = null
  var zipGeoLookup: (Int, String) => ArrayList[ArrayList[String]] = null
  var lookup_quovaipgeoLookup:(String,String) => ArrayList[ArrayList[String]] = null

  def initLookups() {
    zipGeoLookup = defzipGeoLookup("ZipGeo Lookup","zip_geo_intl.dat")
    lookup_quovaipgeoLookup = deflookup_quovaipgeoLookup("Quova IpGeo","quova_ip_geo.dat"); //Empty File For Now
  }
  
  def main(args:Array[String]){
    initLookups();
//   transform(1194267528,"2277.214032","2276.9936445","5.8437891263",9629091,"N",	"N","Y","N","N","Y","Y","N","N","85","N","0", "REGULAR","1","90002","CA","LosAngeles",
//          "33.949057","-118.243238","1","323","34.02919928315412186379928315412186379928","-118.275487813620071684587813620071684588","REGULAR","90002",1,"108.215.188.237","38.896","-77.446","108.215.188","N","quova",1,"79.222195953","virginia","85","38.896","-77.446","Y","Y","Y");
  var ip3geo=List("38.896","-77.446","108.215.188","N","quova","1","79.2221953","virginia","85","38.896","-77.446","");
  var acgeo=List("1","323","34.0291","-118.2754","REGULAR");
  var zipgeo=List("1","90002","CA","LostAngeles","","33.949057","-118.243238");
  transform(1194267528,"2277.214032","2276.9936445","5.8437891263","9629091","N",	"N","Y","N","N","Y","Y","N","N","85","N","0", "REGULAR",zipgeo,acgeo,"90002",1,"108.215.188.237",ip3geo,"Y","Y","Y");
 
  zipgeo=List("1","94134","CA","SanFrancisco","","37.720","-122.4088");
  acgeo=List("1","415","37.84518","-122.4824","REGULAR");
  ip3geo=List("37.649","-122.145","75.18.207","N","both","1","94","CA","94","37.64866","-122.14482")
  transform(1194267587,"15.28165","22.928944","9.5147","9629091","Y","N","N","Y","N","N","Y","N","N","94","Y","0","REGULAR",zipgeo,acgeo,"94134",1,"75.18.207.213",ip3geo,"Y","Y","Y")  
  
  zipgeo=List();
  acgeo=List();
  ip3geo=List("52.373","4.893","91.227.71","N","quova","146","79.222","noord-holland","85","52.373","4.893")
  transform(1194267841,"0","0","0","20770","Y","N","N","Y","N","N","Y","N","N","85","Y","0","UNKNOWN",zipgeo,acgeo,"22807",100,"91.227.71.250",ip3geo,"N","N","N")  
  
  }

  /**
   record
    decimal("|") cntry_id;
    string("|") areacode;
    decimal("|") latitude;
    decimal("|") longitude;
    decimal("\n") phone_type;
  end acgeo = NULL;
  
   record
    decimal("|") latitude;
    decimal("|") longitude;
    string("|") ip3;
    string("|") aol_yn;
    string("|") dset;
    decimal("|") user_cntry_id;
    decimal("|") countryscore;
    string("|") state;
    decimal("|") statescore;
    decimal("|") ip3_lat;
    decimal("|") ip3_long;
    decimal("\n") zipscore;
  end ip3geo = NULL;
   */
def transform(
    user_id:Long, dist_ip3_zip:String, dist_ip3_areacode:String, dist_ac_zip:String, cntry_area:String, dist_ip3_zip_lt_50:String, dist_ip3_zip_bw_50_300:String,
    dist_ip3_zip_gt_300:String, dist_ip3_areacode_lt_50:String, dist_ip3_areacode_bw_50_300:String, dist_ip3_areacode_gt_300:String, dist_ac_zip_lt_50:String,
    dist_ac_zip_bw_50_300:String, dist_ac_zip_gt_300:String, ip3_state_score:String, ip3_state_match:String,ip3_zip_score:String, phone_type:String, 
    zipgeo:List[String],
    acgeo:List[String],
    pstl_code:String, user_cntry_id:Int, user_ip_addr:String, 
    ip3geo:List[String],
    ip3_ac_dist_exists:String,ac_zip_dist_exists:String,ip3_zip_dist_exists:String){
//  def transform(user_id:Long,user_ip_addr: String, user_cntry_id: Int, pstl_code: String, acgeo: Array[String], ip3geo: Array[String]
//		  ,dist_ip3_zip:String,dist_ip3_zip_lt_50:String,dist_ip3_zip_bw_50_300:String,dist_ip3_zip_gt_300:String,dist_ac_zip:String,dist_ac_zip_lt_50:String,
//		  dist_ac_zip_bw_50_300:String,dist_ac_zip_gt_300:String ,dist_ip3_areacode :String,dist_ip3_areacode_lt_50 :String,dist_ip3_areacode_bw_50_300 :String,dist_ip3_areacode_gt_300 :String,
//		  ip3_state_score :String,ip3_state_match :String,ip3_zip_score :String,phone_type :String,ip3_ac_dist_exists :String,ac_zip_dist_exists :String,ip3_zip_dist_exists :String) {
    val tab="\t\t"
    initLookups()
    var ip3_lat = 0.0; /*get the lat/long for the given ip3. --> shouldnt this be initialized to south pole co-ordinates ?*/
    var ip3_long = 0.0;
    var quova_ip3_lat = 0.0; /*get the lat/long for the given ip3. --> shouldnt this be initialized to south pole co-ordinates ?*/
    var quova_ip3_long = 0.0;
    var pstl_lat = 0.0;
    var pstl_long = 0.0;
    var ac_lat = 0.0;
    var ac_long = 0.0;
    var quova_ip3_zip_dist: Double = 999;
    var quova_ip3_ac_dist: Double = 999;
    var ind: Int = string_rindex(user_ip_addr, ".");
    var ip3: String = string_lrtrim(string_substring(user_ip_addr, 1, ind - 1));
    var ip4: String = string_lrtrim(user_ip_addr);
    var quovaip3geo = lookup_quovaipgeoLookup.apply(ip3, ip4);
    var temp_zip: String = null;
    var quova_ip3_zip_dist_exists: String = null;
    var quova_ip3_ac_dist_exists: String = null;
    var expected_timezoneoffset = 0.0;
    var connection_speed: String = "";
    var i: Int = 0;
    var done: Int = 0;
    ip3_lat = if (is_null(ip3geo)) 0.0 else ip3geo.apply(9).toDouble //ip3_lat;
    ip3_long = if (is_null(ip3geo)) 0.0 else ip3geo.apply(10).toDouble //ip3_long;
    quova_ip3_lat = if (quovaip3geo==null ||quovaip3geo.size() ==0 ) 0.0 else quovaip3geo.get(0).get(11).toDouble//latitude;
    quova_ip3_long = if (quovaip3geo ==null || quovaip3geo.size() ==0) 0.0 else quovaip3geo.get(0).get(12).toDouble //longitude;
    if (is_null(quovaip3geo))
      expected_timezoneoffset = ip3_long * 4;
    else
      expected_timezoneoffset = quova_ip3_long * 4;
    if (user_cntry_id != 3)
      temp_zip = string_filter_out(pstl_code, " ");
    else {
      var ind_space = 0;
      temp_zip = string_lrtrim(pstl_code);
      ind_space = string_index(temp_zip, " ");
      if (ind_space != 0)
        temp_zip = string_concat(string_substring(temp_zip, 1, ind_space - 1), string_substring(temp_zip, ind_space + 1, 1));
    }
    var zipgeo = zipGeoLookup(user_cntry_id, string_upcase(temp_zip));
    pstl_lat = if (is_null(zipgeo) || zipgeo.size()==0 ) 0.0 else zipgeo.get(0).get(6).toDouble // latitude;
    pstl_long = if (is_null(zipgeo)) 0.0 else zipgeo.get(0).get(7).toDouble //longitude;
    ac_lat = if (is_null(acgeo)) 0.0 else acgeo.apply(2).toDouble //latitude;
    ac_long = if (is_null(acgeo)) 0.0 else acgeo.apply(3).toDouble //longitude;
    if (is_null(zipgeo) || is_null(quovaip3geo) || tnsar_geo_location_supported(user_cntry_id) < 1) {
      quova_ip3_zip_dist = 0; quova_ip3_zip_dist_exists = "N";
    } else {
      quova_ip3_zip_dist = compute_distance(quova_ip3_lat, quova_ip3_long, pstl_lat, pstl_long, "miles"); quova_ip3_zip_dist_exists = "Y";
    }
    if (is_null(quovaip3geo) || is_null(acgeo) || tnsar_geo_location_supported(user_cntry_id) < 1) {
      quova_ip3_ac_dist = 0; quova_ip3_ac_dist_exists = "N";
    } else {
      quova_ip3_ac_dist = compute_distance(quova_ip3_lat, quova_ip3_long, ac_lat, ac_long, "miles"); quova_ip3_ac_dist_exists = "Y";
    }
    var de_connection_speed=  if( is_null(quovaip3geo) || quovaip3geo.size()==0 ) "" else quovaip3geo.get(0).get(6);
    if ( de_connection_speed.isEmpty() || de_connection_speed == "?")
      connection_speed = "Unknown";
    else if (de_connection_speed == "broadband" || de_connection_speed == "cable" || de_connection_speed == "xdsl" || de_connection_speed == "t1" || de_connection_speed == "t3" || de_connection_speed == "oc3")
      connection_speed = "broadband";
    else
      connection_speed = de_connection_speed;
     var dist_quova_ip3_zip = quova_ip3_zip_dist;
     var dist_quova_ip3_zip_lt_50 = if (quova_ip3_zip_dist < 50 ) "Y" else "N";
     var dist_quova_ip3_zip_bw_50_300 = if (quova_ip3_zip_dist >= 50 && quova_ip3_zip_dist <= 300 ) "Y" else "N";
     var dist_quova_ip3_zip_gt_300 = if (quova_ip3_zip_dist > 300 ) "Y" else "N";
     var  dist_quova_ip3_areacode = quova_ip3_ac_dist;
     var dist_quova_ip3_areacode_lt_50 = if (quova_ip3_ac_dist < 50 ) "Y" else "N";
     var dist_quova_ip3_areacode_bw_50_300 = if (quova_ip3_ac_dist >= 50 && quova_ip3_ac_dist <= 300 ) "Y" else "N";
     var dist_quova_ip3_areacode_gt_300 = if (quova_ip3_ac_dist > 300 ) "Y" else "N";  
     println(user_id+tab+dist_ip3_zip+tab+dist_ip3_areacode+tab+dist_ac_zip+tab+dist_ip3_zip_lt_50+tab+dist_ip3_zip_bw_50_300+tab+dist_ip3_zip_gt_300+
         tab+dist_ip3_areacode_lt_50+tab+dist_ip3_areacode_bw_50_300+tab+dist_ip3_areacode_gt_300+tab+dist_ac_zip_lt_50+tab+dist_ac_zip_bw_50_300+tab+
         dist_ac_zip_gt_300+tab+ip3_state_score+tab+dist_quova_ip3_zip+tab+dist_quova_ip3_areacode+tab+dist_quova_ip3_zip_lt_50+tab+dist_quova_ip3_zip_bw_50_300+tab+
         dist_quova_ip3_zip_gt_300+tab+dist_quova_ip3_areacode_lt_50+tab+dist_quova_ip3_areacode_bw_50_300+tab+dist_quova_ip3_areacode_gt_300+tab+connection_speed+tab+ip3_state_match+
         tab+ip3_zip_score+tab+phone_type+tab+ip3_ac_dist_exists+tab+ac_zip_dist_exists+tab+ip3_zip_dist_exists+tab+quova_ip3_zip_dist_exists+tab+quova_ip3_ac_dist_exists+tab+expected_timezoneoffset)
  }
}