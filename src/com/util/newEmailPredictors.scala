package com.util

object newEmailPredictors {
  val numerics = "0123456789"
  val tab="\t\t"
    
  def main(args:Array[String]){
	 transform(1194267528,"marlenearenas@yahoo.com","Marlene Arenas");
	 transform(1194267532,"yurii0503@yandex.ru","Юрий Бабихин");
	 transform(1194267535,"serebrenikova10@mail.ru","Olga Serebrenikova")
  }
  def transform(user_id: Long,emailIn: String, user_nameIn: String) {
    var email = string_convert_explicit(string_cleanse(emailIn, "!"), "US-ASCII", "#");
    var user_name = string_convert_explicit(string_cleanse(user_nameIn, "!"), "US-ASCII", "#");
    var org = if (string_index(email, "org") != 0) 1 else 0;
    var edu = if (string_index(email, "edu") != 0) 1 else 0;
    var numeric_count = string_length(string_filter(email, numerics));
    var gcs_email_uname = gcs(email, user_name);
    var numeric_in_row = numer_in_a_row(email);
    var numeric_increasing_in_row = numer_inc_in_a_row(email);
    var numeric_equal_in_row = numer_eq_in_a_row(email);
    var numeric_decreasing_in_row = numer_dec_in_a_row(email);
    println(org,tab,edu,tab,numeric_count,tab,gcs_email_uname,tab,numeric_in_row,tab,numeric_increasing_in_row,tab,numeric_equal_in_row,tab,numeric_decreasing_in_row)
  }

  /**
   * This method checks continuous number of numbers in a row.
   */
  def numer_in_a_row(source: String): Int = {
    inPlaceTransform.in_a_row(source, numerics)   
  }

  def numer_inc_in_a_row(source: String): Int = {
    var email_chars = source.toCharArray()
    var max_num_inc_in_row = 0;
    var len = string_length(source);
    var num_inc_in_row = 0;
    var i: Int = 0;
    var prevNum: Double = 0;
    var currNum: Double = 0;
    i = 0;
    while (i < len) {
      if (numerics.contains(email_chars.apply(i))) {
        currNum = email_chars.apply(i);
        if (num_inc_in_row > 0 && currNum == prevNum + 1) {
          num_inc_in_row = num_inc_in_row + 1;
          if (num_inc_in_row > max_num_inc_in_row) max_num_inc_in_row = num_inc_in_row;
          prevNum = currNum;
        } else {
          num_inc_in_row = 1;
          prevNum = currNum;
        }
      } else
        num_inc_in_row = 0;
      i = i + 1;
    }
    max_num_inc_in_row;
  }

  def numer_dec_in_a_row(source: String) = {
    var max_num_inc_in_row = 0;
    var len = string_length(source);
    var num_inc_in_row = 0;
    var i = 0;
    var prevNum = 0;
    var currNum = 0;

    var email_chars = source.toCharArray();
    i = 0;
    while (i < len) {
      if (numerics.contains(email_chars.apply(i))) {
        currNum = email_chars.apply(i);
        if (num_inc_in_row > 0 && currNum == prevNum - 1) {
          num_inc_in_row = num_inc_in_row + 1;
          if (num_inc_in_row > max_num_inc_in_row) max_num_inc_in_row = num_inc_in_row;
          prevNum = currNum;
        } else {
          num_inc_in_row = 1;
          prevNum = currNum;
        }
      } else
        num_inc_in_row = 0;
      i = i + 1;
    }
    max_num_inc_in_row
  }

  def numer_eq_in_a_row(source: String) = {
    var max_num_inc_in_row = 0;
    var len = string_length(source);
    var num_inc_in_row = 0;
    var i = 0;
    var prevNum: Double = 0;
    var currNum: Double = 0;
    var email_chars = source.toCharArray()
    i = 0;
    while (i < len) {
      if (numerics.contains(email_chars.apply(i))) {
        currNum = email_chars.apply(i);
        if (num_inc_in_row > 0 && currNum == prevNum) {
          num_inc_in_row = num_inc_in_row + 1;
          if (num_inc_in_row > max_num_inc_in_row) max_num_inc_in_row = num_inc_in_row;
          prevNum = currNum;
        } else {
          num_inc_in_row = 1;
          prevNum = currNum;
        }
      } else
        num_inc_in_row = 0;
      i = i + 1;
    }
    max_num_inc_in_row
  }

/**
 * Returns the length of Longest common substring between two strings.
 */
  def gcs(email_raw:String, uname:String):Int= {
	  	var s=email_raw.toLowerCase();
	  	var t=uname.toLowerCase();
		if (s.isEmpty() || t.isEmpty()) {
			return 0;
		}
 		var m = s.length();
		var n = t.length();
		var cost = 0;
		var maxLen = 0;
		var p = new Array[Int](n);
		var d = new Array[Int](n);
 		for (i <- 0 until m) {
			for ( j <- 0 until n) {
				// calculate cost/score
				if (s.charAt(i) != t.charAt(j)) {
					cost = 0;
				} else {
					if ((i == 0) || (j == 0)) {
						cost = 1;
					} else {
						cost = p.apply(j - 1) + 1;
					}
				}
				d.update(j,cost);
 				if (cost > maxLen) {
					maxLen = cost;
				}
			}
			var swap = p;
			p = d;
			d = swap;
		}
		return maxLen;
	}

}

