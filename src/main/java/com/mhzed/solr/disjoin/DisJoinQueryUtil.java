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
import com.mhzed.solr.disjoin.dv.NumberDocVal;
import com.mhzed.solr.disjoin.dv.NumbersDocVal;
import com.mhzed.solr.disjoin.dv.StringDocValReader;
import com.mhzed.solr.disjoin.dv.StringsDocValReader;

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
	
	public static DocValReader<?> getDocValReader(String fieldName, FieldType ft) {
    
		switch (parseType(ft)) {
			case Int:	return ft.isMultiValued() ? NumbersDocVal.intReader(fieldName) : NumberDocVal.intReader(fieldName);
			case Long: return ft.isMultiValued() ? NumbersDocVal.longReader(fieldName) : NumberDocVal.longReader(fieldName);
			case Str: return ft.isMultiValued() ? new StringsDocValReader(fieldName) : new StringDocValReader(fieldName);
      default:  // case Double: 
        return ft.isMultiValued() ? NumbersDocVal.doubleReader(fieldName) : NumberDocVal.doubleReader(fieldName);
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
