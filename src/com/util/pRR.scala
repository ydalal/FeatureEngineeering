package com.util

import java.util.ArrayList

object pRR {
  val tab="\t\t"
  var prrLookup:(String)=>ArrayList[String]=null
  
  def initLookups(){
	prrLookup=defPRRLookup("PRR Lookup", "empty.dat");
  }
  
  def main(args:Array[String]){
    initLookups()
    transform(1194267528,"323","692",1);
    transform(1194267535,"734","516",1);    
  }
  
  def transform(user_id:Long,areacode:String,prefix:String,user_cntry_id :Int){
	    var temp1 = areacode;
        var temp2 = prefix;
        var temp:String = null;
        if (user_cntry_id == 1 ||  user_cntry_id == 2)
                temp = string_concat("1",temp1, temp2);        
        if ( user_cntry_id == 136 )
        		temp = string_concat("52",temp1, temp2 );
        var phriskrating = prrLookup(temp);
        var ph_risk_rating = if (is_null(phriskrating)) 0 else phriskrating.get(1).toDouble //ph_risk_rate;   
        var flag_ph_risk_rating = if (is_null(phriskrating)) 0 else 1;    
        println(user_id+tab+ph_risk_rating+tab+flag_ph_risk_rating);
  }
}
