package com.util


//Note :: for guid_creation_date, or other fields the input fields might be "Null" and we should convert them into null
//NOTE: matching long to string will return unexpected results / null results, make sure your convey it through right data types
object visitorAttributes {
  var tab="\t\t"    

    def main(args: Array[String]) {
    transform(1194267528,1,"0",null,"",1,0,null,null,"2013-10-01 00:00:01","0");
    transform(1194267528,2,"0",null,"",168,0,"",null,"2013-10-01 12:00:03","0");
  }
  
  def transform(user_id:Long,row_id:Long,latencyEstimate:String,timezoneoffsetIn:String,httpacceptlanguage:String,user_cntry_id:Int,visit_countIn:Double,last_x_keywords_search:String,guid_creation_date:String,user_cre_date:String,last_used_user_id:String) {
    var timezoneoffset: Long = convertTimezoneoffset(timezoneoffsetIn);
    var temp: String = "";
    var maxLangRisk: Double = createLanguageRisk(httpacceptlanguage, user_cntry_id);
    var latency_estimate_str: String = null;
    var suspendedUserRec = null;
    var latency_estimate= 0.0 
    if (is_null(latencyEstimate) || latencyEstimate.isEmpty() || string_index(latencyEstimate, "H") > 0)
      latency_estimate = 0;
    else
      latency_estimate = latencyEstimate.toDouble;    
    if (latency_estimate < 500)
      latency_estimate_str = "latencyestimate_lt_500";
    else if (latency_estimate >= 500 && latency_estimate < 1000)
      latency_estimate_str = "latencyestimate_bw_500_1000";
    else if (latency_estimate >= 1000 && latency_estimate < 2000)
      latency_estimate_str = "latencyestimate_bw_1000_2000";
    else if (latency_estimate >= 2000 && latency_estimate < 3000)
      latency_estimate_str = "latencyestimate_bw_2000_3000";
    else if (latency_estimate >= 3000)
      latency_estimate_str = "latencyestimate_gt_3000";
    var visit_count = if ( is_null(visit_countIn) || visit_countIn.isNaN() ) 0  else visit_countIn ; //is_blank(visit_countIn)
    val has_last_x_keywords_search =  if (is_null(last_x_keywords_search) || is_blank(last_x_keywords_search)) "N" else "Y";
    val min_x_keywords_search =  if (is_null(last_x_keywords_search) || is_blank(last_x_keywords_search)) 0 else string_length(string_filter(last_x_keywords_search, "|")) ;
    val has_last_guid_creation = if(is_null(guid_creation_date) || is_blank(guid_creation_date)) "N" else "Y";        
    val min_since_last_guid_creation = if(is_null(guid_creation_date)|| is_blank(guid_creation_date)) 0 else datetime_difference_minutes(user_cre_date, guid_creation_date);
    val has_used_search = if(is_null(last_x_keywords_search) || is_blank(last_x_keywords_search)) "N" else "Y";
    val has_last_used_user_id = if(is_null(last_used_user_id)|| is_blank(last_used_user_id) || last_used_user_id == "0"  ) "N" else "Y";//TODO:: Error: MAke sure all == 0 implementation should be == "0"
    val has_timezoneoffset = if( is_null(timezoneoffsetIn) ) "N" else "Y";//?
    timezoneoffset = if(timezoneoffset == -9999 || is_null(timezoneoffset) ) -9999 else  timezoneoffset * -1; 
    val latency_estimateOut = latency_estimate_str;
    val language_risk = maxLangRisk;    
    println(row_id+tab+user_id+tab+visit_count+tab+has_last_x_keywords_search+tab+min_x_keywords_search+tab+min_since_last_guid_creation+tab+has_last_guid_creation+tab+has_used_search+tab+has_last_used_user_id+tab+timezoneoffset+tab+has_timezoneoffset+tab+latency_estimateOut+tab+latency_estimate+tab+language_risk);
  }
}