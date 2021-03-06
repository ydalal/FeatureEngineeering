package com.util

import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.util.ArrayList

/**
 * @author ydalal
 */
object gibberishFeatures {
  val lhs = "qwertasdfgzxcv"
  val rhs = "yuiophjklbnm";
  val digits = "0123456789"
  val numbers = digits
  val vowels = "aeiouAEIOU"
  val titles = Array("attn:", "attn", "mrs.", "miss", "mme.", "mr.", "ms.", "mrs", "mme", "mr", "ms")
  val vowels_and_y = "aeiouyAEIOUY";
  val consonants = "bcdfghjklmnpqrstvwxz";
  val all_char = "abcdefghijklmnopqrstuvwxyz0123456789`-=~!@#$%^&*()_+[]\\{}|;':\",./<>?";
  val days_of_week = Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat");
  val debug = true
  val lkpDelim = "#"
  var dowLookup: (Integer, String) => Double = null
  var todLookup: (Int, Int) => Double = null
  val tab = "\t"

  def initLookups() {
    dowLookup = defdayofweeklookup("DayOfWeek Lookup", "empty.dat")
    todLookup = deftimeofdaylookup("TimeOfDay Lookup", "empty.dat")
  }

  case class brgRecord1(addr: String, blng_curncy_id: Int, city: String, dayphone: String, email: String, gender_mfu: String, last_modfd: String, nightphone: String, pref_categ_interest1_id: Int,
    pref_categ_interest2_id: Int, pstl_code: String, state: String, user_cntry_id: Int, user_flags: String)
  case class brgRecord2(user_id: String, user_name: String, user_regn_id: String, user_site_id: String, user_slctd_id: String, uvdetail: String, uvrating: String, user_confirm_code: String, user_sex_flag: String, user_ip_addr: String, req_email_count: String, payment_type: String,
    date_confirm: String, nbr_stored_address: String, flagsex2: String, date_of_birth: String)
  case class brgRecord3(aol_master_id: String, tax_id: String, linked_paypal_acct: String, paypal_link_state: String, flagsex3: String, cre_date: String, user_cre_date: String, flagsex4: String, flagsex5: String, flagsex6: String, weight: String, equifax_sts: String, upd_date: String, addr1: String,
    addr2: String, areacode: String, prefix: String, row_id: String)

  def main(args: Array[String]) {
    initLookups()
    var path = "/Users/ydalal/" + File.separator + "address_consistencyIn_Tab.txt"
    var br = getFileReader(path);
    var run = true
    var count = 0
    while (run) {
      val str = br.readLine()
      if (str == null) {
        run = false
      } else { //Read File Line by Line
        try {
          var split = str.split("\t")
          if (split.length == 48) {
            var r1 = brgRecord1(split.apply(0), split.apply(1).toInt, split.apply(2), split.apply(3), split.apply(4), split.apply(5), split.apply(6), split.apply(7), split.apply(8).toInt, split.apply(9).toInt, split.apply(10), split.apply(11), split.apply(12).toInt, split.apply(13));
            var r2 = brgRecord2(split.apply(14), split.apply(15), split.apply(16), split.apply(17), split.apply(18), split.apply(19), split.apply(20), split.apply(21), split.apply(22), split.apply(23), split.apply(24), split.apply(25), split.apply(26), split.apply(27), split.apply(28), split.apply(29))
            var r3 = brgRecord3(split.apply(30), split.apply(31), split.apply(32), split.apply(33), split.apply(34), split.apply(35), split.apply(36), split.apply(37), split.apply(38), split.apply(39), split.apply(40), split.apply(41), split.apply(42), split.apply(43), split.apply(44), split.apply(45), split.apply(46), split.apply(47))
            /*
			12 Colmns Set
			addr, blng_curncy_id, city, dayphone, email, gender_mfu, last_modfd, nightphone, pref_categ_interest1_id, pref_categ_interest2_id, pstl_code, state 
			user_cntry_id, user_flags, user_id, user_name, user_regn_id, user_site_id, user_slctd_id, uvdetail, uvrating, user_confirm_code, user_sex_flag, user_ip_addr,
			req_email_count, payment_type, date_confirm, nbr_stored_address, flagsex2, date_of_birth, aol_master_id, tax_id, linked_paypal_acct, paypal_link_state, flagsex3, cre_date,
			user_cre_date, flagsex4, flagsex5, flagsex6, weight equifax_sts, upd_date, addr1, addr2, areacode, prefix, row_id
			*/
            //Step 2 : Reformat Input Data Streams
            var addr = r1.addr.replaceAll("\0", "") //Note :: IMP:: There are invisible       "     <NUL><NUL><NUL>" padding in first token, addr, not sure why, but we can replace them with following replaceall for now. 
            var email = r1.email
            var user_slctd_id = r2.user_slctd_id
            var pstl_code = r1.pstl_code
            var user_name = r2.user_name
            var dayphone = r1.dayphone
            var gender_mfu = r1.gender_mfu;
            var equifax_sts = r3.equifax_sts
            var city = r1.city
            var state = r1.state
            var linked_paypal_acct = r3.linked_paypal_acct
            var user_ip_addr = r2.user_ip_addr
            var user_confirm_code = r2.user_confirm_code
            var tax_id = r3.tax_id
            val charset = "ISO-8859-1"
            var nightphone = r1.nightphone
            addr = string_convert_explicit(addr, charset, "X")
            city = string_convert_explicit(city, charset, "X")
            dayphone = string_convert_explicit(dayphone, charset, "X")
            email = string_convert_explicit(email, charset, "X")
            equifax_sts = string_convert_explicit(equifax_sts, charset, "X")
            gender_mfu = string_convert_explicit(gender_mfu, charset, "X")
            nightphone = string_convert_explicit(nightphone, charset, "X")
            pstl_code = string_convert_explicit(pstl_code, charset, "X")
            state = string_convert_explicit(state, charset, "X")
            user_name = string_convert_explicit(user_name, charset, "X")
            user_slctd_id = string_convert_explicit(user_slctd_id, charset, "X")
            user_confirm_code = string_convert_explicit(user_confirm_code, charset, "X")
            user_ip_addr = string_convert_explicit(user_ip_addr, charset, "X")
            tax_id = string_convert_explicit(tax_id, charset, "X")
            linked_paypal_acct = string_convert_explicit(linked_paypal_acct, charset, "X")

            val user_id = r2.user_id
            val date_of_birth = r2.date_of_birth
            val user_cre_date = r3.user_cre_date
            val user_cntry_id: Int = r1.user_cntry_id
            val upd_date = r3.upd_date //Made an error here, it was being sent empty
            val date_confirm = r2.date_confirm
            val user_flags = r1.user_flags
            val user_sex_flag = r2.user_sex_flag
            val flagsex2 = r2.flagsex2
            val flagsex3 = r3.flagsex3
            val flagsex4 = r3.flagsex4
            val flagsex5 = r3.flagsex5
            val flagsex6 = r3.flagsex6
            val user_site_id = r2.user_site_id.toInt
            var names = extract_names(string_filter_out(user_name.toLowerCase(), numbers)) //names extraction looks good
            val firstname = names.get(0)
            val lastname = names.get(1)
            val f2 = try { flagsex2.toInt } catch { case _: Exception => 0 };
            val f3 = try { flagsex3.toInt } catch { case _: Exception => 0 }
            val f4 = try { flagsex4.toInt } catch { case _: Exception => 0 }
            val f5 = try { flagsex5.toInt } catch { case _: Exception => 0 }
            val f6 = try { flagsex6.toInt } catch { case _: Exception => 0 }
            reformat(addr, email, user_slctd_id, pstl_code, firstname, user_name, lastname, dayphone, user_id, date_of_birth, user_cre_date, equifax_sts, user_cntry_id, gender_mfu, upd_date, date_confirm, user_flags, user_sex_flag, f2, f3, f4, f5, f6, user_site_id)
            count += 1
            if (count == 100) {
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

  /**
   * Counts characters in Source string that are present in Target string.
   */
  def countCommonChars(src: String, tgt: String): Integer = {
    var count: Integer = 0
    val inChar = src.toCharArray()
    for (i <- 0 until inChar.length)
      if (tgt.contains(inChar.apply(i))) {
        count += 1
      }
    count
  }

  /**
   * src: input string
   * output: Returns number of characters in input string that are present in left hand side keys of keyboard.
   */
  def count_lhs(src: String): Integer = {
    countCommonChars(src, lhs)
  }

  /**
   *  src: input string
   *  output: Returns number of characters in input string that are present in left hand side keys of keyboard.
   */
  def count_rhs(src: String): Integer = {
    countCommonChars(src, rhs)
  }

  /**
   * This function would count the number of non-overlapping occurences of the search in source string.
   * Source: Target String to be searched in.
   * Search: String to be searched.
   * Note : Implementation using java functions.
   * Test Cases:
   *  num_str_in_str("hello","l") : 2
   *  num_str_in_str("hhh","h") : 3
   *  num_str_in_str("ahhh","h") : 3
   *  num_str_in_str("ahdfdsfdfhh","h") : 3
   *  num_str_in_str("","") : 0
   *  num_str_in_str("asf","d") : 0
   */
  def num_str_in_str(source: String, search: String): Integer = {
    var index = -1
    var current_string = source
    var num_matches = 0
    val search_len = search.length()
    var i = 0
    var len = source.length()
    while (i < len) {
      current_string = current_string.substring(i, len)
      len = current_string.length()
      index = current_string.indexOf(search)
      if (index != -1) {
        num_matches += 1
        i = index + search_len
      } else
        i = len
    }
    num_matches
  }

  /**
   * This method returns the maximum number of continuous occurence of a character in string source, that is present in list_chars.
   * Example :
   * same_in_a_row("88882","0123456789"): 4
   * same_in_a_row("88882","abcdefghijklmnopqrstuvwxyz"): 1
   * same_in_a_row("88882","82"): 1 ??
   *
   * Method Test cases
   *
   */
  def same_in_a_row(source: String, list_chars: String): Integer = {
    var max_num_in_row = 0;
    var singleSource = source.toLowerCase().toCharArray()
    var num_in_row = 0
    var last_max_num_char: Char = ' '
    var i = 0
    var len: Integer = source.length()
    while (i < len) {
      var currChar = singleSource.apply(i)
      if (test_characters_any(currChar, list_chars.toCharArray()) != 0 && (currChar == last_max_num_char))
        num_in_row = num_in_row + 1;
      else {
        if (num_in_row > max_num_in_row)
          max_num_in_row = num_in_row;
        num_in_row = 1;
        last_max_num_char = singleSource.apply(i);
      }
      i = i + 1;
      if (num_in_row > max_num_in_row) {
        max_num_in_row = num_in_row
      }
    }
    max_num_in_row
  }

  def any_numeric(src: String): Integer = {
    countCommonChars(src, digits)
  }

  def any_vowel(src: String): Integer = {
    countCommonChars(src, vowels)
  }

  /**
   * Return maximum number of consecutive characters in sources string, that are present in target string.
   * TODO: Rename this to, max in a sequence.
   */
  def in_a_row(source: String, tgt: String): Integer = {
    val src = source.toLowerCase().toCharArray()
    var max_num_in_a_row = 0
    var num_in_row = 0

    for (i <- 0 until src.length) {
      if (tgt.contains(src.apply(i))) {
        num_in_row += 1
      } else {
        if (num_in_row > max_num_in_a_row)
          max_num_in_a_row = num_in_row
        num_in_row = 0
      }
    }
    if (num_in_row > max_num_in_a_row) max_num_in_a_row = num_in_row

    max_num_in_a_row
  }

  /**
   * This function returns the longest increasing contiguous array of numbers.
   * It returns -1 if the input string contains anything else but numbers.
   */
  def digits_a_seq(source: String): Integer = {
    var max_num_in_seq = -1
    var len = string_length(source);
    if (string_length(string_filter_out(source, "0123456789")) == 0 && len > 1) {
      var single_source = source.toLowerCase().toCharArray() //integer(1) []
      var num_in_seq = 0;
      var i = 0;
      var last_digit = 0;
      max_num_in_seq = 0;
      while (i < len) {
        if (single_source.apply(i) == (last_digit + 1)) {
          num_in_seq = num_in_seq + 1;
          max_num_in_seq = num_in_seq + 1; //fixme, kill this line
        } else
          num_in_seq = 0;
        last_digit = single_source.apply(i);
        i = i + 1;
        if (num_in_seq > max_num_in_seq)
          max_num_in_seq = num_in_seq;
      }
    }
    max_num_in_seq
  }

  def word_match(str: String, words: String, ignore_1_letter_word: Int): String = {
    val tokens = words.split(" ")
    var result = "N"
    for (i <- 0 until tokens.length) {
      val word = tokens.apply(i)
      if (str.indexOf(word) > -1 && !word.isEmpty() && (ignore_1_letter_word == 0 || word.length() > 1)) {
        result = "Y"
      }
    }
    result
  }

  //TODO: It is a basic implementation, it can be further improved and expanded to all countries, as opposed to just 1,3
  def is_zip_expected_format(cntry_id: Int, zip: String): String = {
    var zip_expected_format = "N"
    if (cntry_id == 1) {
      if (zip.length() < 5 || zip.length() > 9 || (string_filter_out(zip, digits).length() != 0))
        zip_expected_format = "N"
      else
        zip_expected_format = "Y"
    } else if (cntry_id == 3) {
      if (zip.length() < 2 || zip.length() > 7 || (string_filter_out(zip.toLowerCase(), "abcdefghijklmnopqrstuvwxyz0123456789").length() != 0))
        zip_expected_format = "N"
      else
        zip_expected_format = "Y"
    }
    zip_expected_format
  }

  def num_begin(str: String): String = {
    if (string_filter(str.substring(0, 1), "0123456789").length() == 1) "Y" else "N";
  }

  def num_mid(str: String): String = {
    if (string_filter(str.substring(0, 1), digits).length() == 0
      && string_filter(str.substring(str.length() - 1, str.length()), digits).length() == 0
      && string_filter(str, digits).length() != 0)
      "Y"
    else
      "N"
  }

  def num_end(str: String): String = {
    if (string_filter(str.substring(str.length() - 1, str.length()), digits).length() != 0)
      "Y"
    else
      "N"
  }

  /**
   * Extract first name and last name from user name.
   * Return Array[String]=<firstname,lastname>
   */
  def extract_names(origname: String): ArrayList[String] = {
    var newname: String = "";
    var lastname: String = "";
    var firstname: String = "";
    var name1: String = "";
    var name2: String = "";
    var name3: String = "";
    var name4: String = "";
    var found_1: Int = 0;
    var n_1: Int = 0;
    var i_1: Int = 0;
    var index_1: Int = 0;
    var index1_1: Int = 0;
    var index2_1: Int = 0;
    var index3_1: Int = 0;
    var index4: Int = 0;
    var m_1: Int = 0;
    var found_2: Int = 0;
    var n_2: Int = 0;
    var i_2: Int = 0;
    var index_2: Int = 0;
    var index1_2: Int = 0;
    var index2_2: Int = 0;
    var index3_2: Int = 0;
    var index4_1: Int = 0;
    var m_2: Int = 0;
    var found_3: Int = 0;
    var n_3: Int = 0;
    var i_3: Int = 0;
    var index_3: Int = 0;
    var index1_3: Int = 0;
    var index2_3: Int = 0;
    var index3_3: Int = 0;
    var index4_2: Int = 0;
    var m_3: Int = 0;
    var found_4: Int = 0;
    var n_4: Int = 0;
    var i_4: Int = 0;
    var index_4: Int = 0;
    var index1_4: Int = 0;
    var index2_4: Int = 0;
    var index3_4: Int = 0;
    var index4_3: Int = 0;
    var m_4: Int = 0;
    var found_5: Int = 0;
    var n_5: Int = 0;
    var i_5: Int = 0;
    var index_5: Int = 0;
    var index1_5: Int = 0;
    var index2_5: Int = 0;
    var index3_5: Int = 0;
    var index4_4: Int = 0;
    var m_5: Int = 0;
    var found_6: Int = 0;
    var n_6: Int = 0;
    var i_6: Int = 0;
    var index_6: Int = 0;
    var index1_6: Int = 0;
    var index2_6: Int = 0;
    var index3_6: Int = 0;
    var index4_5: Int = 0;
    var m_6: Int = 0;
    var found_7: Int = 0;
    var n_7: Int = 0;
    var i_7: Int = 0;
    var index_7: Int = 0;
    var index1_7: Int = 0;
    var index2_7: Int = 0;
    var index3_7: Int = 0;
    var index4_6: Int = 0;
    var m_7: Int = 0;

    if (!is_blank(origname) && test_characters_any(origname, numbers) == 0) {
      var found: Int = 0;
      var n = titles.length;
      var i = 0;
      var index = 0;
      var index1 = 0;
      var index2 = 0;
      var index3 = 0;
      var index4 = 0;

      newname = string_filter_out(origname, "_");

      for (i <- 0 until n if found == 0) {
        if (string_index(newname, titles.apply(i)) == 1) {
          var m = string_length(titles.apply(i));
          newname = string_ltrim(string_substring(newname, 2 + m, length_of(newname) - m));
          found = 1;
        }
      }

      newname = string_replace(newname, "\t", " ");

      index = string_index(newname, "(");
      if (index > 0) {
        newname = string_substring(newname, 1, index - 1);
      }

      index = string_index(newname, "c/o");
      if (index > 0) {
        newname = string_substring(newname, 1, index - 1);
      }

      n = string_length(newname);
      index = string_index(newname, ".");

      if ((index > 0) && (index < n) && string_substring(newname, index + 1, 1) != " ")
        newname = string_replace_first(newname, ".", " ");

      index1 = string_index(newname, " ");
      name1 = string_substring(newname, 1, index1 - 1);
      name2 = string_ltrim(string_substring(newname, index1 + 1, n));

      index2 = string_index(name2, " ");
      name3 = string_ltrim(string_substring(name2, index2 + 1, n));
      name2 = string_ltrim(string_substring(name2, 1, index2));

      index3 = string_index(name3, " ");
      name4 = string_ltrim(string_substring(name3, index3 + 1, n));
      name3 = string_ltrim(string_substring(name3, 1, index3));

      if (string_length(name3) == 1 || string_index(name3, ".") == 2) {
        name3 = name4;
        name4 = "";
      }

      if ((string_index(name1, ".") > 0 || string_length(name1) == 1 || name1 == "LT" || name1 == "PT" || name1 == "DR") && (string_length(name3) != 0)) {
        name1 = string_trim(name1) + " " + name2;
        name2 = name3;
      }

      index = string_index(name1, ",");
      if (index != 0) {
        name4 = string_substring(name1, 1, index - 1);
        name1 = name2;
        name2 = name3;
      }

      if ((string_index(name4, ".") != 0) || ((string_length(name4) >= 2 && (5 + string_index("ii__ iii__iv___v____vi___vii__viii_ix___md___phd__dds__", name4)) % 5 == 1))) {
        name3 = string_trim(name3) + " " + name4;
        name4 = "";
      } else if ((string_index(name1, ",") != 0)) {
        name2 = string_trim(name2) + " " + name3;
        name3 = "";
      }

      if (!is_blank(name4)) {
        if ((5 + string_index("ann__jo___anne_joe__anna_jane_marie", name2)) % 5 == 1) {
          firstname = string_trim(name1) + " " + string_trim(name2);
          lastname = name4;
        } else if ((5 + string_index("von__le___de___da___", name3)) % 5 == 1) {
          firstname = name1;
          lastname = string_trim(name3) + " " + string_trim(name4);
        } else {
          firstname = name1;
          lastname = name4;
        }

      } else if (!is_blank(name3)) {
        if ((5 + string_index("ann__jo___anne_joe__anna_jane_marie", name2)) % 5 == 1) {
          firstname = string_trim(name1) + " " + string_trim(name2);
          lastname = name4;
        } else if ((5 + string_index("von__le___de___da___", name2)) % 5 == 1) {
          firstname = name1;
          lastname = string_trim(name2) + " " + string_trim(name3);
        } else {
          firstname = name1;
          lastname = name3;
        }
      } else {
        lastname = name2;
        firstname = name1;
      }
    }
    var out = new ArrayList[String]()
    out.add(firstname); out.add(lastname);
    out
  }

  def strdave(str: String): String = {
    val temp = str.trim()
    if ((string_index(temp, " n ") != 0) || (string_index(temp, "n ") != 0) || (string_index(temp, " e ") != 0) || (string_index(temp, "e ") != 0) ||
      (string_index(temp, " w ") != 0) || (string_index(temp, "w ") != 0) || (string_index(temp, " s ") != 0) || (string_index(temp, "s ") != 0) ||
      (string_index(temp, " north ") != 0) || (string_index(temp, "north ") != 0) || (string_index(temp, " north") != 0) || (string_index(temp, " east ") != 0) || (string_index(temp, "east ") != 0) || (string_index(temp, " east") != 0) ||
      (string_index(temp, " west ") != 0) || (string_index(temp, "west ") != 0) || (string_index(temp, " west") != 0) || (string_index(temp, " south ") != 0) || (string_index(temp, "south ") != 0) || (string_index(temp, " south") != 0) ||
      (string_index(temp, " drive ") != 0) || (string_index(temp, "drive ") != 0) || (string_index(temp, " drive") != 0) || (string_index(temp, " dr ") != 0) || (string_index(temp, "dr ") != 0) || (string_index(temp, " dr") != 0) ||
      (string_index(temp, " street ") != 0) || (string_index(temp, "street ") != 0) || (string_index(temp, " street") != 0) || (string_index(temp, " st ") != 0) || (string_index(temp, "st ") != 0) || (string_index(temp, " st") != 0) ||
      (string_index(temp, " str ") != 0) || (string_index(temp, "str ") != 0) || (string_index(temp, " str") != 0) || (string_index(temp, " way ") != 0) || (string_index(temp, "way ") != 0) || (string_index(temp, " way") != 0) ||
      (string_index(temp, " avenue ") != 0) || (string_index(temp, "avenue ") != 0) || (string_index(temp, " avenue") != 0) || (string_index(temp, " ave ") != 0) || (string_index(temp, "ave ") != 0) || (string_index(temp, " ave") != 0) ||
      (string_index(temp, " boulevard ") != 0) || (string_index(temp, "boulevard ") != 0) || (string_index(temp, " boulevard") != 0) || (string_index(temp, " blvd ") != 0) || (string_index(temp, "blvd ") != 0) || (string_index(temp, " blvd") != 0) ||
      (string_index(temp, " circle ") != 0) || (string_index(temp, "circle ") != 0) || (string_index(temp, " circle") != 0) || (string_index(temp, " cir ") != 0) || (string_index(temp, "cir ") != 0) || (string_index(temp, " cir") != 0) ||
      (string_index(temp, " cr ") != 0) || (string_index(temp, "cr ") != 0) || (string_index(temp, " cr") != 0) || (string_index(temp, " rd ") != 0) || (string_index(temp, "rd ") != 0) || (string_index(temp, " rd") != 0) ||
      (string_index(temp, " road ") != 0) || (string_index(temp, "road ") != 0) || (string_index(temp, " road") != 0) || (string_index(temp, " lane ") != 0) || (string_index(temp, "lane ") != 0) || (string_index(temp, " lane") != 0) ||
      (string_index(temp, " ln ") != 0) || (string_index(temp, "ln ") != 0) || (string_index(temp, " ln") != 0) || (string_index(temp, " rue ") != 0) || (string_index(temp, "rue ") != 0) || (string_index(temp, " rue") != 0) ||
      (string_index(temp, " des ") != 0) || (string_index(temp, "des ") != 0) || (string_index(temp, " des") != 0) || (string_index(temp, " plaza ") != 0) || (string_index(temp, "plaza ") != 0) || (string_index(temp, " plaza") != 0) ||
      (string_index(temp, " pl ") != 0) || (string_index(temp, "pl ") != 0) || (string_index(temp, " pl") != 0) || (string_index(temp, " via ") != 0) || (string_index(temp, "via ") != 0) || (string_index(temp, " via") != 0) ||
      (string_index(temp, " terrace ") != 0) || (string_index(temp, "terrace ") != 0) || (string_index(temp, " terrace") != 0) || (string_index(temp, " highway ") != 0) || (string_index(temp, "highway ") != 0) || (string_index(temp, " highway") != 0) ||
      (string_index(temp, " hgwy ") != 0) || (string_index(temp, "hgwy ") != 0) || (string_index(temp, " hgwy") != 0) || (string_index(temp, " hgw ") != 0) || (string_index(temp, "hgw ") != 0) || (string_index(temp, " hgw") != 0) ||
      (string_index(temp, " ct ") != 0) || (string_index(temp, "ct ") != 0) || (string_index(temp, " ct") != 0) || (string_index(temp, " court ") != 0) || (string_index(temp, "court ") != 0) || (string_index(temp, " court") != 0) ||
      (string_index(temp, " lot ") != 0) || (string_index(temp, "lot ") != 0) || (string_index(temp, " lot") != 0) || (string_index(temp, " alley ") != 0) || (string_index(temp, "alley ") != 0) || (string_index(temp, " alley") != 0) ||
      (string_index(temp, " allee ") != 0) || (string_index(temp, "allee ") != 0) || (string_index(temp, " allee") != 0) || (string_index(temp, " expressway ") != 0) || (string_index(temp, "expressway ") != 0) || (string_index(temp, " expressway") != 0) ||
      (string_index(temp, " expwy ") != 0) || (string_index(temp, "expwy ") != 0) || (string_index(temp, " expwy") != 0) || (string_index(temp, " expw ") != 0) || (string_index(temp, "expw ") != 0) || (string_index(temp, " expw") != 0) ||
      (string_index(temp, " route ") != 0) || (string_index(temp, "route ") != 0) || (string_index(temp, " route") != 0) || (string_index(temp, " rt ") != 0) || (string_index(temp, "rt ") != 0) || (string_index(temp, " rt") != 0) ||
      (string_index(temp, " pmb ") != 0) || (string_index(temp, "pmb ") != 0) || (string_index(temp, " pmb") != 0) || (string_index(temp, " blok ") != 0) || (string_index(temp, "blok ") != 0) || (string_index(temp, " blok") != 0) ||
      (string_index(temp, " block ") != 0) || (string_index(temp, "block ") != 0) || (string_index(temp, " block") != 0) || (string_index(temp, " place ") != 0) || (string_index(temp, "place ") != 0) || (string_index(temp, " place") != 0)) {
      "Y"
    } else
      "N"
  }

  /**
   * Reformat Input String.
   */
  def reformat(addr: String, email: String, user_slctd_id: String, pstl_code: String, firstname: String, user_name: String, lastname: String, dayphone: String, user_id: String,
    date_of_birth: String, user_cre_date: String, equifax_sts: String, user_cntry_id: Int, gender_mfu: String, upd_date: String, date_confirm: String, user_flags: String, user_sex_flag: String,
    flagsex2: Int, flagsex3: Int, flagsex4: Int, flagsex5: Int, flagsex6: Int, user_site_id: Integer) = {
    var addr_low = string_downcase(addr)
    var email_before_amper = string_substring(string_downcase(email), 1, string_index(email, "@") - 1);
    var email_domain = string_substring(string_downcase(email), string_index(email, "@") + 1, string_length(email)); /*gets available chars*/
    var len_email_before_amper = string_length(email_before_amper);
    var email_num_of_vowel = string_length(string_filter(email_before_amper, "aeiou"));
    var user_id_low = string_downcase(user_slctd_id);
    var user_id_len = string_length(user_slctd_id);
    var ui_num_of_vowel_with_y = string_length(string_filter(user_id_low, "aeiouy"));
    var ui_num_of_vowel_without_y = string_length(string_filter(user_id_low, "aeiou"));
    var ui_num_of_consonants = string_length(string_filter(user_id_low, "bcdfghjklmnpqrstvwxz"));
    var ui_num_of_asdf = string_length(string_filter(user_id_low, "asdf"));
    var email_num_of_asdf = string_length(string_filter(email_before_amper, "asdf"));
    var ui_num_numeric = string_length(string_filter(user_id_low, "0123456789"));
    var ui_num_of_letters = string_length(string_filter(user_id_low, "abcdefghijklmnopqrstuvwxyz"));
    var ui_vowels_in_a_row = in_a_row(user_id_low, vowels);
    var ui_vowels_and_y_in_a_row = in_a_row(user_id_low, vowels_and_y);
    var ui_con_in_a_row = in_a_row(user_id_low, consonants);
    var ui_num_same_in_a_row = same_in_a_row(user_id_low, all_char);
    var email_num_of_vowels_with_y = string_length(string_filter(email_before_amper, "aeiouy"));
    var email_num_of_consonants = string_length(string_filter(email_before_amper, "bcdfghjklmnpqrstvwxz"));
    var email_num_numeric = string_length(string_filter(email_before_amper, "0123456789"));
    var email_num_of_letters = string_length(string_filter(email_before_amper, "abcdefghijklmnopqrstuvwxyz"));
    var email_vowels_in_a_row = in_a_row(email_before_amper, vowels);
    var email_vowels_and_y_in_a_row = in_a_row(email_before_amper, vowels_and_y);
    var email_con_in_a_row = in_a_row(email_before_amper, consonants);
    var email_numofsamecharinarow = same_in_a_row(email_before_amper, all_char);
    var zip_num_with_dash = string_length(string_filter_out(string_downcase(pstl_code), "0123456789abcdefghijklmnopqrstuvwxyz-"));
    var zip_num_without_dash = string_length(string_filter_out(string_downcase(pstl_code), "0123456789abcdefghijklmnopqrstuvwxyz"));
    var zip_sanitized = string_filter_out(string_downcase(pstl_code), " -");
    var zip_sanitized_len = string_length(string_filter_out(string_downcase(pstl_code), " -"));
    var addr_num_numeric = string_length(string_filter(addr_low, "0123456789"));
    var addr_num_of_letters = string_length(string_filter(addr_low, "abcdefghijklmnopqrstuvwxyz"));
    var addr_len = string_length(string_filter_out(addr_low, " "));
    var addr_num_same_in_row = same_in_a_row(addr_low, all_char);
    var name_down = string_downcase(user_name);
    var first_down = string_lrtrim(firstname);
    var last_down = string_lrtrim(lastname);
    var len_first = string_length(first_down);
    var len_last = string_length(last_down);
    var len_name_down = string_length(string_filter_out(first_down + last_down, " "));
    var first_name_num_vowels = string_length(string_filter(first_down, "aeiou"));
    var last_name_num_vowels = string_length(string_filter(last_down, "aeiou"));
    var first_name_num_vowels_with_y = string_length(string_filter(first_down, "aeiouy"));
    var last_name_num_vowels_with_y = string_length(string_filter(last_down, "aeiouy"));
    var name_num_vowels = string_length(string_filter(first_down + last_down, "aeiou"));
    var name_num_vowels_with_y = string_length(string_filter(first_down + last_down, "aeiouy"));
    var ph_num_digits = string_length(string_filter(dayphone, "0123456789"));
    var ph_num_len = string_length(dayphone);
    var ext_index = string_index(dayphone, "ext");
    var ph_num_excl_ext = if (ext_index != 0)
      string_concat(string_substring(dayphone, 1, ext_index - 1),
        string_substring(dayphone, ext_index + 1, 9999))
    else
      dayphone;
    var ph_non_numeric_excl_ext = string_length(string_filter_out(ph_num_excl_ext, "0123456789"));
    var first_down_vec: Array[Char] = first_down.toCharArray();
    var last_down_vec = last_down.toCharArray();
    var name_down_vec = name_down.toCharArray();
    var count_nums_username = string_length(string_filter(name_down, "0123456789"));
    var fname_vowels_in_a_row = in_a_row(first_down, vowels);
    var fname_vowels_and_y_in_a_row = in_a_row(first_down, vowels_and_y);
    var fname_con_in_a_row = in_a_row(first_down, consonants);
    var fname_num_same_in_a_row = same_in_a_row(first_down, all_char);
    var lname_vowels_in_a_row = in_a_row(last_down, vowels);
    var lname_vowels_and_y_in_a_row = in_a_row(last_down, vowels_and_y);
    var lname_con_in_a_row = in_a_row(last_down, consonants);
    var lname_num_same_in_a_row = same_in_a_row(last_down, all_char);
    var userid_length = string_length(user_slctd_id);
    var email_percofvowel: Float = if (len_email_before_amper == 0) 0 else (email_num_of_vowel * 1.0f) / len_email_before_amper;
    var email_percofsamecharinarow: Float = if (len_email_before_amper == 0) 0 else (email_numofsamecharinarow * 1.0f) / len_email_before_amper;
    var addr_numofseqnum = digits_a_seq(string_split(addr_low, " ").apply(0));
    var nbr_non_num = string_length(string_filter_out(dayphone, "0123456789"));
    var userid_username = word_match(user_id_low, name_down, 1);
    var email_numbegin = if (any_numeric(string_substring(email, 1, 1)) == 1) "Y" else "N";
    var email_userslctdid = if (string_index(email_before_amper, user_slctd_id) != 0) "Y" else "N";
    var addr_startwithnumeric = if (any_numeric(string_substring(addr, 1, 1)) != 0 != 0) "Y" else "N";
    var addr_uplowcase = if (addr == string_upcase(addr)) "1"
    else if (addr == addr_low) "2"
    else "3";
    var md_0101 = if (is_null(date_of_birth)) "N" else if (string_substring(date_of_birth, 6, 5) == "01-01") "Y" else "N";
    var day_of_week = date_day_of_week(user_cre_date);
    var daypct = dowLookup(user_cntry_id, day_of_week);
    var email_domain_yahoo = if (string_index(string_downcase(email_domain), "yahoo") != 0) "Y" else "N";
    var addr_pobox = if (string_index(string_filter_out(addr_low, " ."), "pobox") != 0) "Y" else "N";
    var addr_box = if (string_index(string_filter_out(addr_low, " "), "box") != 0) "Y" else "N";
    var addr_strdave = strdave(addr_low);
    var userid_numofvowely = ui_num_of_vowel_with_y;
    var userid_numofconsonant = ui_num_of_consonants;
    var userid_numofasdf = ui_num_of_asdf;
    var userid_percofvowel = if (user_id_len == 0) 0 else ui_num_of_vowel_without_y * 1.0f / user_id_len;
    var userid_percofvowely = if (user_id_len == 0) 0 else ui_num_of_vowel_with_y * 1.0f / user_id_len;
    var userid_percofconsonant = if (user_id_len == 0) 0 else ui_num_of_consonants * 1.0f * 1.0f / user_id_len;
    var userid_percofasdf = if (user_id_len == 0) 0 else ui_num_of_asdf * 1.0f / user_id_len;
    var userid_numofnumeric = ui_num_numeric;
    var userid_percofnumeric = if (user_id_len == 0) 0 else ui_num_numeric * 1.0f / user_id_len;
    var userid_numofletter = ui_num_of_letters;
    var userid_percofletter = if (user_id_len == 0) 0 else ui_num_of_letters * 1.0f / user_id_len;
    var userid_numofvowelinarow = ui_vowels_in_a_row;
    var userid_numofvowelyinarow = ui_vowels_and_y_in_a_row;
    var userid_numofconsonantinarow = ui_con_in_a_row;
    var userid_numofsamecharinarow = ui_num_same_in_a_row;
    var userid_percofvowelinarow = if (user_id_len == 0) 0 else ui_vowels_in_a_row * 1.0f / user_id_len;
    var userid_percofvowelyinarow = if (user_id_len == 0) 0 else ui_vowels_and_y_in_a_row * 1.0f / user_id_len;
    var userid_percofconsonantinarow = if (user_id_len == 0) 0 else ui_con_in_a_row * 1.0f / user_id_len;
    var userid_percofsamecharinarow = if (user_id_len == 0) 0 else ui_num_same_in_a_row * 1.0f / user_id_len;
    var email_numofvowely = email_num_of_vowels_with_y;
    var email_numofconsonant = email_num_of_consonants;
    var email_numofasdf = email_num_of_asdf;
    var email_percofvowely = if (len_email_before_amper == 0) 0 else email_num_of_vowels_with_y * 1.0f / len_email_before_amper;
    var email_percofconsonant = if (len_email_before_amper == 0) 0 else email_num_of_consonants * 1.0f / len_email_before_amper;
    var email_percofasdf = if (len_email_before_amper == 0) 0 else email_num_of_asdf * 1.0f / len_email_before_amper;
    var email_numofnumeric = email_num_numeric;
    var email_percofnumeric = if (len_email_before_amper == 0) 0 else email_num_numeric * 1.0f / len_email_before_amper;
    var email_numofletter = email_num_of_letters;
    var email_percofletter = if (len_email_before_amper == 0) 0 else email_num_of_letters * 1.0f / len_email_before_amper;
    var email_numofvowelinarow = email_vowels_in_a_row;
    var email_numofvowelyinarow = email_vowels_and_y_in_a_row;
    var email_numofconsonantinarow = email_con_in_a_row;
    var email_percofvowelinarow = if (len_email_before_amper == 0) 0 else email_vowels_in_a_row * 1.0f / len_email_before_amper;
    var email_percofvowelyinarow = if (len_email_before_amper == 0) 0 else email_vowels_and_y_in_a_row * 1.0f / len_email_before_amper;
    var email_percofconsonantinarow = if (len_email_before_amper == 0) 0 else email_con_in_a_row * 1.0f / len_email_before_amper;
    var zip_nonalphanum_nodash = zip_num_with_dash;
    var zip_nonalphanum = zip_num_without_dash;
    var zip_length = zip_sanitized_len;
    var zip_expected_format = is_zip_expected_format(user_cntry_id, zip_sanitized);
    var addr_numofnumeric = addr_num_numeric;
    var addr_numofchar = addr_num_of_letters;
    var addr_percofnumeric = if (addr_len == 0) 0 else (addr_num_numeric * 1.0f / addr_len);
    var addr_percofchar = if (addr_len == 0) 0 else (addr_num_of_letters * 1.0f / addr_len);
    var addr_length = addr_len;
    var addr_numofsamenum = same_in_a_row(addr_low, digits);
    var addr_numofsamecharinarow = addr_num_same_in_row;
    var addr_percofsamecharinarow = if (addr_len == 0) 0 else (addr_num_same_in_row * 1.0f / addr_len);
    var nbr_abcd = num_str_in_str(name_down, "abcd");
    var nbr_abcde = num_str_in_str(name_down, "abcde");
    var nbr_aaaa = num_str_in_str(name_down, "aaaa");
    var nbr_bbbb = num_str_in_str(name_down, "bbbb");
    var nbr_cccc = num_str_in_str(name_down, "cccc");
    var nbr_eeee = num_str_in_str(name_down, "eeee");
    var nbr_v_fname = first_name_num_vowels;
    var nbr_v_lname = last_name_num_vowels;
    var nbr_vy_fname = first_name_num_vowels_with_y;
    var nbr_vy_lname = last_name_num_vowels_with_y;
    var nbr_r_fname = string_length(string_filter(first_down, rhs));
    var nbr_l_fname = string_length(string_filter(first_down, lhs));
    var nbr_r_lname = string_length(string_filter(last_down, rhs));
    var nbr_l_lname = string_length(string_filter(last_down, lhs));
    var max_v_fname = in_a_row(first_down, vowels);
    var max_v_lname = in_a_row(last_down, vowels);
    var nbr_same_fname = same_in_a_row(first_down, all_char);
    var nbr_same_lname = same_in_a_row(last_down, all_char);
    var nbr_v_tot = name_num_vowels;
    var nbr_vy_tot = name_num_vowels_with_y;
    var nbr_r_tot = string_length(string_filter(name_down, rhs));
    var nbr_l_tot = string_length(string_filter(name_down, lhs));
    var pct_v_fname = if (len_first == 0) 0 else (first_name_num_vowels * 1.0f / len_first) * 100;
    var pct_vy_fname = if (len_first == 0) 0 else (first_name_num_vowels_with_y * 1.0f / len_first) * 100;
    var pct_r_fname = if (len_first == 0) 0 else (string_length(string_filter(first_down, rhs)) * 1.0f / len_first) * 100;
    var pct_l_fname = if (len_first == 0) 0 else (string_length(string_filter(first_down, lhs)) * 1.0f / len_first) * 100;
    var pct_max_vf = if (len_first == 0) 0 else (in_a_row(first_down, vowels) * 1.0f / len_first) * 100;
    var pct_v_lname = if (len_last == 0) 0 else (last_name_num_vowels * 1.0f / len_last) * 100;
    var pct_vy_lname = if (len_last == 0) 0 else (last_name_num_vowels_with_y * 1.0f / len_last) * 100;
    var pct_r_lname = if (len_last == 0) 0 else (string_length(string_filter(last_down, rhs)) * 1.0f / len_last) * 100;
    var pct_l_lname = if (len_last == 0) 0 else (string_length(string_filter(last_down, lhs)) * 1.0f / len_last) * 100;
    var pct_max_vl = if (len_last == 0) 0 else (in_a_row(last_down, vowels) * 1.0f / len_last) * 100;
    var pct_v_tot = if (len_name_down == 0) 0 else ((first_name_num_vowels + last_name_num_vowels) * 1.0f / len_name_down) * 100;
    var pct_vy_tot = if (len_name_down == 0) 0 else ((first_name_num_vowels_with_y + last_name_num_vowels_with_y) * 1.0f / len_name_down) * 100;
    var pct_l_tot = if (len_name_down == 0) 0 else (string_length(string_filter(name_down, lhs)) * 1.0f / len_name_down) * 100;
    var pct_r_tot = if (len_name_down == 0) 0 else (string_length(string_filter(name_down, rhs)) * 1.0f / len_name_down) * 100;
    var total_length = len_name_down;
    var nbr_num = ph_num_digits;
    var nbr_del = ph_num_len - ph_num_digits;
    var nbr_non_numeric_excl_ext = ph_non_numeric_excl_ext;
    var len_day_phone = ph_num_len;
    var userid_dot = if (string_index(user_slctd_id, ".") != 0) "Y" else "N";
    var userid_underline = if (string_index(user_slctd_id, "_") != 0) "Y" else "N";
    var userid_dash = if (string_index(user_slctd_id, "-") != 0) "Y" else "N";
    var userid_numbegin = num_begin(user_slctd_id);
    var userid_nummid = num_mid(user_slctd_id);
    var userid_numend = num_end(user_slctd_id);
    var email_domain_hotmail = if (string_index(string_downcase(email_domain), "hotmail") != 0) "Y" else "N";
    var email_domain_aol = if (string_index(string_downcase(email_domain), "aol") != 0) "Y" else "N";
    var email_dot = if (string_index(email_before_amper, ".") != 0) "Y" else "N";
    var email_underline = if (string_index(email_before_amper, "_") != 0) "Y" else "N";
    var email_dash = if (string_index(email_before_amper, "-") != 0) "Y" else "N";
    var email_nummid = num_mid(email_before_amper);
    var email_numend = num_end(email_before_amper);
    var email_username = word_match(email_before_amper, name_down, 1);
    var equifax_status = if (!is_null(equifax_sts) && string_lrtrim(string_downcase(equifax_sts)) == "y") 1 else 0;
    var gender = if (string_lrtrim(string_downcase(gender_mfu)) == "m") 1 else 0;
    var timediff_created_modified = if (is_null_extended(upd_date) || is_null_extended(user_cre_date)) 0 else datetime_difference_seconds(upd_date, user_cre_date);
    var timediff_now_confirmation = if (is_null_extended(date_confirm) || is_null_extended(user_cre_date)) 0 else datetime_difference_seconds(date_confirm, user_cre_date);
    var hour_of_day = datetime_hour(user_cre_date); /*hour of day */
    var hrpct = todLookup(user_cntry_id, datetime_hour(user_cre_date));
    var userid_numofvowel = ui_num_of_vowel_without_y;
    var country_code = tnsar_get_country_code(user_site_id);
    val email_numofvowel = email_num_of_vowel
    println(user_id + tab + userid_length + tab + email_numofvowel + tab + email_percofvowel + tab + email_numofsamecharinarow + tab +
      email_percofsamecharinarow + tab + addr_numofseqnum + tab + nbr_non_num + tab + userid_username + tab + email_numbegin + tab +
      email_userslctdid + tab + addr_startwithnumeric + tab + addr_uplowcase + tab + md_0101 + tab + day_of_week + tab + daypct +
      tab + email_domain_yahoo + tab + addr_pobox + tab + addr_box + tab + addr_strdave + tab + userid_numofvowel + tab + userid_numofvowely + tab +
      userid_numofconsonant + tab + userid_numofasdf + tab + userid_percofvowel + tab + userid_percofvowely + tab + userid_percofconsonant + tab +
      userid_percofasdf + tab + userid_numofnumeric + tab + userid_percofnumeric + tab + userid_numofletter + tab + userid_percofletter + tab +
      userid_numofvowelinarow + tab + userid_numofvowelyinarow + tab + userid_numofconsonantinarow + tab + userid_numofsamecharinarow + tab + userid_percofvowelinarow +
      tab + userid_percofvowelyinarow + tab + userid_percofconsonantinarow + tab + userid_percofsamecharinarow + tab + email_numofvowely + tab + email_numofconsonant + tab +
      email_numofasdf + tab + email_percofvowely + tab + email_percofconsonant + tab + email_percofasdf + tab + email_numofnumeric + tab + email_percofnumeric + tab +
      email_numofletter + tab + email_percofletter + tab + email_numofvowelinarow + tab + email_numofvowelyinarow + tab + email_numofconsonantinarow + tab + email_percofvowelinarow
      + tab + email_percofvowelyinarow + tab + email_percofconsonantinarow + tab + zip_nonalphanum_nodash + tab + zip_nonalphanum + tab + zip_length + tab + zip_expected_format
      + tab + addr_numofnumeric + tab + addr_numofchar + tab + addr_percofnumeric + tab + addr_percofchar + tab + addr_length + tab + addr_numofsamenum + tab + addr_numofsamecharinarow
      + tab + addr_percofsamecharinarow + tab + nbr_abcd + tab + nbr_abcde + tab + nbr_aaaa + tab + nbr_bbbb + tab + nbr_cccc + tab + nbr_eeee + tab + nbr_v_fname + tab +
      nbr_v_lname + tab + nbr_vy_fname + tab + nbr_vy_lname + tab + nbr_r_fname + tab + nbr_r_lname + tab + nbr_l_fname + tab + nbr_l_lname + tab + max_v_fname + tab + max_v_lname
      + tab + nbr_same_fname + tab + nbr_same_lname + tab + nbr_v_tot + tab + nbr_vy_tot + tab + nbr_r_tot + tab + nbr_l_tot + tab + pct_v_fname + tab + pct_vy_fname + tab +
      pct_r_fname + tab + pct_l_fname + tab + pct_max_vf + tab + pct_v_lname + tab + pct_vy_lname + tab + pct_r_lname + tab + pct_l_lname + tab + pct_max_vl + tab + pct_v_tot +
      tab + pct_vy_tot + tab + pct_l_tot + tab + pct_r_tot + tab + total_length + tab + count_nums_username + tab + nbr_num + tab + nbr_del + tab + nbr_non_numeric_excl_ext + tab
      + len_day_phone + tab + userid_dot + tab + userid_underline + tab + userid_dash + tab + userid_numbegin + tab + userid_nummid + tab + userid_numend + tab + email_domain_hotmail
      + tab + email_domain_aol + tab + email_dot + tab + email_underline + tab + email_dash + tab + email_nummid + tab + email_numend + tab + email_username + tab + equifax_status
      + tab + gender + tab + timediff_created_modified + tab + timediff_now_confirmation + tab + user_flags + tab + user_sex_flag + tab + flagsex2 + tab + flagsex3 + tab + flagsex4
      + tab + flagsex5 + tab + flagsex6 + tab + hour_of_day + tab + hrpct + tab + country_code + tab + fname_vowels_in_a_row + tab + fname_vowels_and_y_in_a_row + tab +
      fname_con_in_a_row + tab + fname_num_same_in_a_row + tab + lname_vowels_in_a_row + tab + lname_vowels_and_y_in_a_row + tab + lname_con_in_a_row + tab + lname_num_same_in_a_row)
  }
}
