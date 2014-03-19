package com.util

object distancFunctions {
	val radius_of_earth_miles:Double = 3963.1d ;
	val radius_of_earth_km = 6378 ;
	val degrees_radians_conversion:Double = 57.29577951d ;
	val tab="\t"
  def compute_distance(lat1:Double, long1:Double, lat2:Double, long2:Double, units:String):Double ={
		  var rLat1 = lat1/degrees_radians_conversion;
		  var rLong1 = long1/degrees_radians_conversion;
		  var rLat2 = lat2/degrees_radians_conversion;
		  var rLong2 = long2/degrees_radians_conversion;
		  var earth_radius = if ( units == "km") radius_of_earth_km else radius_of_earth_miles ;
		  var distance = 999.0;
		  var temp = ( math_cos(rLat1) * math_cos(rLong1) * math_cos(rLat2) * math_cos(rLong2) )+ (math_cos(rLat1) * math_sin(rLong1) * math_cos(rLat2) * math_sin(rLong2)) + (math_sin(rLat1) * math_sin(rLat2));
		if ( temp > 1.0 ) temp = 1.0;
		if ( temp < -1.0 ) temp = -1.0;
		distance = math_acos(temp)*earth_radius;
		distance
	}
}