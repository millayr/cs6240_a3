/**
 * @author Ryan Millay
 * @author Nikit Waghela
 * CS6240
 * Assignment 3
 */

package airdel.a3.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Parser {
	private static final String PARSER_PROP = "/parser.properties";
	private static final String DEL = "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final String HEADERPROP = "header";
	private static final String KEYPROP = "keys";  // rpm
	private static final String HEADERSPLIT = ",";
	private static final String KEYSPLIT = ",";  // rpm
	private static final String INTERMEDIATEKEYSPLIT = "_";  // rpm
	private static final String IS_VALID = "is_valid";
	private static final String SEP = "_";
	private static final Object PAIRSEP = "|";
	public static int MAX_HEAD_LEN = 120;
	private static boolean _ISLOAD = false;
	public static String HEADERS[];
	public static List<String[]> KEYS = new ArrayList<String[]>();  // rpm
	private String del = ",";
	public Map<String, Object> data;
	
	static {
		if(!_ISLOAD) {
			try {
				//load_headers();
				load_headers_and_keys();
			} catch (Exception e) { e.printStackTrace();}
		}
		_ISLOAD = true;
	}
	/**
	 * Loads the headers form /parser.properties
	 * @throws IOException
	 */
	@SuppressWarnings("all")
	private static void load_headers() throws IOException {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = Parser.class.getResourceAsStream(PARSER_PROP);
			prop.load(in);
			String header = prop.getProperty(HEADERPROP);
			HEADERS = header.replaceAll("\"", "").split(HEADERSPLIT);
		} catch (Exception io) {
			HEADERS = new String[MAX_HEAD_LEN];
			
			for(Integer i = 0; i <  MAX_HEAD_LEN; i++) {
				HEADERS[i] = i.toString();
			}
			io.printStackTrace();
		} finally {
			try {
				in.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Loads the headers and keys from /parser.properties
	 * @throws IOException
	 */
	private static void load_headers_and_keys() throws IOException {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = Parser.class.getResourceAsStream(PARSER_PROP);
			prop.load(in);
			
			// read the headers of the csv file
			String header = prop.getProperty(HEADERPROP);
			HEADERS = header.replaceAll("\"", "").split(HEADERSPLIT);
			
			// read the key permutation for this trial
			String key = prop.getProperty(KEYPROP);
			String[] intermediateKeys = key.split(KEYSPLIT);
			for(String intermediateKey : intermediateKeys) {
				KEYS.add(intermediateKey.split(INTERMEDIATEKEYSPLIT));
			}
		} catch (Exception io) {
			HEADERS = new String[MAX_HEAD_LEN];
			
			for(Integer i = 0; i <  MAX_HEAD_LEN; i++) {
				HEADERS[i] = i.toString();
			}
			io.printStackTrace();
		} finally {
			try {
				in.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Constructor creates the Parser, takes a char DELIMITER
	 * as an argument
	 * @param del
	 */
	public Parser (char del) {
		this.del = del+"";
		data = new HashMap<String, Object>();
		for (String header :HEADERS) {
			data.put(header, null);
		}
		data.put(IS_VALID, false);
	}
	
	/**
	 * Parses the line String by splitting it using DELIMITER.
	 * It avoids splitting DELIMITER between quotes.
	 * @param line
	 * @return
	 */
	public Parser parse (String line) {
		data.put(IS_VALID, false);
		if(line == null)
			return this;
		String[] splitOut = line.trim().split(del+DEL);
		if(splitOut.length < HEADERS.length)
			return this;
		int i = 0;
		for(String x: splitOut) {
			try {
				data.put(HEADERS[i], Integer.parseInt(x));
			} catch (Exception e) {
				data.put(HEADERS[i], x);
			}
			i++;
		}
		data.put(IS_VALID, true);
		return this;
	}
	
	/**
	 * Parses the value at String index and returns Value as
	 * Integer
	 * @param index String key
	 * @return value Integer value
	 * @throws NumberFormatException
	 */
	public int getInt (String index) 
			throws NumberFormatException {
		try {
			return (Integer) data.get(index);
		} catch (NumberFormatException e) {
			throw e;
		}
	}
	
	/**
	 * Returns the value as string for the given String index
	 * @param index String key
	 * @return value String value
	 */
	public String getText (String index) {
		Object obj = data.get(index);
		if(obj == null)
			return null;
		return obj.toString();
	}
	
	/**
	 * Returns length of the data
	 * @return
	 */
	public int getLength(){
		return data.size();
	}
	
	public String getKeyValuePairs(String indices[]) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for(String key: indices) {
			if(i != 0)
				sb.append(PAIRSEP);
			sb.append(getKeyValuePair(key));
			i = 1;
		}
		return sb.toString();
	}
	
	public String getKeyValuePair(String index) {
		return index+SEP+data.get(index).toString();
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return data.toString();
	}
	
	/**
	 * Returns true if the data is consistent
	 * Decided by parser.parse(String&lt;line&gt;).  
	 * @return
	 */
	public boolean isValid() {
		return ((Boolean) data.get(IS_VALID));
	}
}