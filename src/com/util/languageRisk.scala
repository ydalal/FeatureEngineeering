package com.util

import com.ebay.tnsar.scala.brg.lookupsDef._
import java.util.ArrayList

object languageRisk {
  var langRiskLookup: (String, Int) => Double = null
  var langLookup: (String) => ArrayList[String] = null

  def createLanguageRisk(httpacceptlanguage: String, user_cntry_id: Int): Double =
    {
      if (langRiskLookup == null)
        langRiskLookup = defLangRisklookup("LanguageCountryRisk Lookup", "Language_CountryRisk.dat");
      if (langLookup == null)
        langLookup = defAcceptLanguage("AcceptLanguage Lookup", "lang_lookup.final.dat");
      var maxLangRisk = 0.0;
      var i = 0;
      var lang = langLookup.apply(httpacceptlanguage)
      if (!is_null(lang)) {
        var langs = string_split(lang.get(2), ","); //lang.language
        for (i <- 0 until langs.length) {
          var subLangs = string_split(langs.apply(i), ";");
          if (subLangs.length > 0) {
            var langrisk = langRiskLookup.apply(string_downcase(string_trim(subLangs.apply(0))), user_cntry_id);
            if (!is_null(langrisk) && langrisk > maxLangRisk)
              maxLangRisk = langrisk
          }
        }
      }
      if (maxLangRisk == 0.0) {
        var langrisk = langRiskLookup.apply("en-us", 1);
        maxLangRisk = langrisk;
      }
      maxLangRisk;
    }
}
