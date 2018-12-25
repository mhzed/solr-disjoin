package com.mhzed.solr.disjoin;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;

import com.mhzed.solr.disjoin.dv.DocValReader;
import com.mhzed.solr.disjoin.dv.DoubleDocValReader;
import com.mhzed.solr.disjoin.dv.IntDocValReader;
import com.mhzed.solr.disjoin.dv.LongDocValReader;
import com.mhzed.solr.disjoin.dv.StringDocValReader;

public class DisJoinQueryUtil {
	
	// supported data types to be joined on 
	public enum CompatibleDataType {
		Int, Long, Str, Double
	};
	
	public static CompatibleDataType parseType(FieldType ft) {
		if (ft.getClassArg().contains("IntPointField")) return CompatibleDataType.Int;
		else if (ft.getClassArg().contains("LongPointField")) return CompatibleDataType.Long;
		else if (ft.getClassArg().contains("DoublePointField")) return CompatibleDataType.Double;
		else if (ft.getClassArg().contains("StrField")) return CompatibleDataType.Str;
		else throw new RuntimeException("Uppported field type " + ft.getTypeName());
	}
	
	public static DocValReader<?> getDocValReader(String fieldName, CompatibleDataType type) {
		switch (type) {
			case Int:	return new IntDocValReader(fieldName);
			case Long: return new LongDocValReader(fieldName);
			case Str: return new StringDocValReader(fieldName);
			case Double: return new DoubleDocValReader(fieldName);
			default: return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static Query createSetQuery(String field, FieldType ft, Set<?> set) {
    Query q = null;
    if (set.size() == 0) return new MatchNoDocsQuery();
		CompatibleDataType type = parseType(ft); 
		typeCheck(set.iterator().next(), type);
		switch (type) {
			case Int:	
				q = IntPoint.newSetQuery(field, (Set<Integer>)set);
				break;
			case Long: 
				q = LongPoint.newSetQuery(field, (Set<Long>)set);
				break;
			case Str:
				q = new TermInSetQuery(field, set.stream().map((id)->new BytesRef(id.toString()))
								.collect(Collectors.toList()));
				break;
			case Double: 
				q = DoublePoint.newSetQuery(field, (Set<Double>)set);
				break;
			default: 
				break;
		}
		return q;
	}

	public static void typeCheck(Object e, CompatibleDataType type) {
		if ( 	(type == CompatibleDataType.Int && (e instanceof Integer)) ||
					(type == CompatibleDataType.Long && (e instanceof Long)) ||
					(type == CompatibleDataType.Str && (e instanceof String)) ||
					(type == CompatibleDataType.Double && (e instanceof Double))
						) {
			return;
		}
		throw new ClassCastException("type " + type.name() + " is not compatible with " + e.getClass().getName());
	}
			
}
