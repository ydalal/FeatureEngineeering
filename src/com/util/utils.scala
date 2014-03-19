package com.util

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream

object utils {
	/**
	 * Method to get buffered reader for reading text files generated from hadoop.
	 */
  def getFileReader(path:String):BufferedReader={
    var reader = new InputStreamReader(new FileInputStream(path), "UTF-8");
    var br = new BufferedReader(reader);
    br
  }

}