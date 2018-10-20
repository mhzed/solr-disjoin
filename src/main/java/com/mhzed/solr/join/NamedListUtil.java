package com.mhzed.solr.join;

import org.apache.solr.common.util.NamedList;

public class NamedListUtil {

	// get key, if not exist return defaultVal, if defaultVal is null then throws exception 
	public static String getOr(NamedList<?> args, final String key, final String defaultVal) {
		Object obj = args.get(key);
		if (null != obj)
			return obj.toString();
		else {
			if (defaultVal != null)
				return defaultVal;
			else
				throw new IllegalArgumentException(key + " must be set!");
		}
	}

}
