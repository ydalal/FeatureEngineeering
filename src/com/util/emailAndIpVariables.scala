package com.util

import java.util.ArrayList
import java.io.FileWriter
import java.io.File

object emailAndIpVariables {
  var dir: String = null;
  var keyDelim: String = null
  var ip3rr: (String) => ArrayList[String] = null
  var countryMatchSite: (Integer) => ArrayList[String] = null
  var ip4rr: (String) => ArrayList[String] = null
  var badIp: (String) => ArrayList[String] = null
  var domainscore: (String) => ArrayList[String] = null
  var ipproxy: (String) => ArrayList[String] = null
  var masterIpGeo: (String) => ArrayList[String] = null
  var countryDomain: (String) => ArrayList[String] = null
  var freeDomains: (String) => ArrayList[String] = null
  val tab = "\t\t"

    def initLookups() {
    countryMatchSite = defcountryMatchSite("CountryMatchSite Lookup", "CountryMatchSite.dat");
    ip4rr = defip4rr("IP4RR Lookup", "ip4_201310.dat"); //Confirm
    ip3rr = defip3rr("Ip3RR Lookup", "empty.dat") 
    badIp = defbadIp("BadIPRR Lookup", "badip.dat");
    masterIpGeo = defmasterIpGeo("MasterIpGeo Lookup", "masteripgeo.dat");
    domainscore = defdomainscore("DomainScore Lookup", "dmscore.dat");
    ipproxy = defIpProxy("IPProxy Lookup", "IPProxy.dat");
    countryDomain = defCountryDomain("CountryDomains Lookup", "countrydomains.dat")
    freeDomains = defFreeDomains("FreeDomains Lookup", "freedomains.dat");
  }

  def main(args: Array[String]) {
    //    transform(1194267532,"94.51.110.251","yurii0503@yandex.ru",168,0)
    initLookups();
    var path = "/Users/ydalal/BuyerRegScalaETL"
    var fileWrite = new File(path + File.separator + "address_consistencyOut_TabScala.txt");
    if (write) { if (fileWrite.exists()) fileWrite.delete(); }
    var fw = new FileWriter(fileWrite);
    var br = getFileReader(path + File.separator + "address_consistencyIn_Tab.txt")
    var str: String = null
    var count = 0; var goodRecCnt = 0;
    var colLen = 0
    var flag = true
    while (flag) {
      str = br.readLine
      if (str == null)
        flag = false
      else {
        try {
          var split = str.split("\t")
          if (split.length != 48) {
            count += 1;
          } else {
            /*
			12 Colmns Set
			addr, blng_curncy_id, city, dayphone, email, gender_mfu, last_modfd, nightphone, pref_categ_interest1_id, pref_categ_interest2_id, pstl_code, state 
			user_cntry_id, user_flags, user_id, user_name, user_regn_id, user_site_id, user_slctd_id, uvdetail, uvrating, user_confirm_code, user_sex_flag  
			user_ip_addr, req_email_count, payment_type, date_confirm, nbr_stored_address, flagsex2, date_of_birth, aol_master_id, tax_id, linked_paypal_acct, paypal_link_state, flagsex3
			cre_date, user_cre_date, flagsex4, flagsex5, flagsex6, weight equifax_sts, upd_date, addr1, addr2, areacode, prefix, row_id
			*/
            var user_id = split.apply(14).toLong
            var user_cntry_id = split.apply(12).toInt
            var email: String = split.apply(4)
            var user_ip_addr: String = split.apply(23);
            var user_site_id = split.apply(17).toInt
            transform(user_id, user_ip_addr, email, user_cntry_id, user_site_id);
            goodRecCnt += 1
            if (goodRecCnt == 100)
              flag = false
          }
        } catch {
          case e: Exception => if (debug1) println("Warning:Dropping Record Id : ");
        }
      }
    }
    println("\n\nRecords Count where column width is not 47, is = " + count + "\t Good records Count = ")
    fw.close();
  }

  def transform(user_id: Long, user_ip_addr: String, email: String, user_cntry_id: Int, user_site_id: Int) {
    var regip_ip_score = 0.0;
    var regip_ip_susp_rr = 0.0;
    var regip_ip_ato_rr = 0.0;
    var email_domain_score = 0.0;
    var reg_ip_score_avail = 0.0;
    var regip_ip_addr_cntry_match: String = "Y";
    var regip_ip_match_site: String = "Y";
    var regip_badip: String = "Y";
    var email_domain_ext_cntry = "Y";
    var email_domain_free = "Y";
    var email_domainip_aol = "1";
    var ind: Int = string_rindex(user_ip_addr, ".");
    var ip3: String = string_lrtrim(string_substring(user_ip_addr, 1, ind - 1));
    var email_domain: String = string_upcase(string_substring(email, string_index(email, "@") + 1, string_length(email)));
    var email_domain_ext: String = string_upcase(string_substring(email, string_rindex(email, ".") + 1, string_length(email)));
    var ip3score = ip3rr(ip3);
    var crosscntryrisk: Double = crossCountryRisk.crossCountryLkp(user_cntry_id, user_site_id);
    var ip4_score = ip4rr(user_ip_addr);
    var badip = badIp.apply(user_ip_addr)
    var email_dm_score_rec = domainscore(email_domain);
    var free_email = freeDomains(email_domain)
    var country_domain = countryDomain(email_domain_ext);
    var ip3geo = masterIpGeo(ip3);
    var regip_proxy_info = ipproxy(user_ip_addr);
    regip_ip_score = if (is_null(ip3score)) 0.0 else ip3score.get(2).toDouble //ip3_risk_rating;
    regip_ip_susp_rr = if (is_null(ip3score)) 0.0 else ip3score.get(1).toDouble //ip3_risk_prob;
    regip_ip_ato_rr = if (is_null(ip3score)) 0.0 else ip3score.get(3).toDouble //ip3_ato_risk;
    reg_ip_score_avail = if (is_null(ip3score)) 0 else 1;
    email_domain_score = if (is_null(email_dm_score_rec)) 0.0 else email_dm_score_rec.get(4).toDouble //score;
    regip_badip = if (is_null(badip)) "N" else "Y";
    email_domain_free = if (!is_null(free_email) || (string_index(email_domain, "YAHOO") > 0)) "Y" else "N";
    if (string_index(email_domain, "AOL") >= 1)
      if (is_null(ip3geo)) email_domainip_aol = "5";
      else if (!is_null(ip3geo) && ip3geo.get(3) == "Y") email_domainip_aol = "1";
      else email_domainip_aol = "2";
    else if (is_null(ip3geo)) email_domainip_aol = "5";
    else if (!is_null(ip3geo) && ip3geo.get(3) == "Y") email_domainip_aol = "3";
    else email_domainip_aol = "4";
    email_domain_ext_cntry = if (is_null(country_domain)) "N" else "Y";
    regip_ip_addr_cntry_match = if (is_null(ip3geo)) "N" else if (!is_null(ip3geo) && ip3geo.get(5).toInt == user_cntry_id) "Y" else "N";
    if (!is_null(ip3geo)) {
      var country_site = countryMatchSite(user_cntry_id);
      regip_ip_match_site = "9";
      if (is_null(country_site) && !is_null(ip3geo) && (ip3geo.get(5).toInt != 1 || ip3geo.get(5).toInt != 23 || ip3geo.get(5).toInt != 104))
        regip_ip_match_site = "9";
      else {
        if (ip3geo.get(5).toInt == 1)
          if (user_site_id == 0)
            regip_ip_match_site = "1";
          else
            regip_ip_match_site = "0";
        else if (ip3geo.get(5).toInt == 23)
          if (user_site_id == 23 || user_site_id == 123)
            regip_ip_match_site = "1";
          else
            regip_ip_match_site = "0";

        else {
          if (!is_null(country_site) && user_site_id == country_site.get(1).toInt) //site_id)
            regip_ip_match_site = "1";
          else
            regip_ip_match_site = "0";
        }
      }
    } else regip_ip_match_site = "9";
    val regip_proxy = if (!is_null(regip_proxy_info)) "Y" else "N";
    val regip_proxy_status = if (!is_null(regip_proxy_info)) regip_proxy_info.get(2) else "none"; //status
    var ip4Score = if (!is_null(ip4_score)) ip4_score.get(1).toDouble else -99.0;
    println(user_id + tab + reg_ip_score_avail + tab + regip_ip_score + tab + regip_ip_susp_rr + tab + regip_ip_ato_rr + tab + email_domain_score + tab + ip4Score + tab + regip_ip_addr_cntry_match + tab + regip_ip_match_site
      + tab + regip_badip + tab + regip_proxy + tab + regip_proxy_status + tab + email_domain_ext_cntry + tab + email_domain_free + tab + email_domainip_aol)
  }

}