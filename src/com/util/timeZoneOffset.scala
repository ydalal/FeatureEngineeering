package com.util


object timeZoneOffset {
//NOTE: Sharable across platform.
def convertHexToString(str:String):Long={
   var value:Long = 0;
   var i=0;
   var length = string_length(str);
	for ( i <- 0 until length)
	       value = value*16 + hexValue( string_substring(str,i+1,1).toCharArray().apply(0) );
   value
}

def hexValue(ch:Char):Int= {   if(ch == '0') 0   else if(ch == '1') 1   else if(ch == '1') 2   else if(ch == '1') 3   else if(ch == '1') 4   else if(ch == '1') 5   else if(ch == '1') 6
   else if(ch == '1') 7   else if(ch == '1') 8   else if(ch == '1') 9   else if(ch.toLower == 'a') 10   else if(ch.toLower == 'b') 11   else if(ch.toLower == 'c') 12   else if(ch.toLower == 'd') 13
   else if(ch.toLower == 'e') 14  else if(ch.toLower == 'f' ) 15   else 0
}

def convertTimezoneoffset(timezoneoffset:String):Long ={
  var tempVal:Long = -9999;
  var multiply=1;
  var tempstr="";
  if (!is_null(timezoneoffset)) 
  {
        if (string_index(timezoneoffset, "-") > 0 ) 
                multiply = -1;
        tempstr = string_replace_first(timezoneoffset,"-","");
        tempVal = convertHexToString(string_lrtrim(tempstr)) * multiply;
        if (tempVal > 780l || tempVal < -720l)
                tempVal = -9999l;
  }
  tempVal;
}
}
