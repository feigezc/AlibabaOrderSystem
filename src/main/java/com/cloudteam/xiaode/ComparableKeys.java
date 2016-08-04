package com.cloudteam.xiaode;

import java.util.List;

public class ComparableKeys implements Comparable<ComparableKeys> {

//	private List<String> orderingKeys;
	private String orderingKeys;
	private Row row;
	// 是否降序
    private boolean descending;
    public ComparableKeys(String orderingKeys, Row row, boolean descd) {
    	if (orderingKeys == null) {
    		throw new RuntimeException("Bad ordering keys, there is a bug maybe");
	    }
	    this.orderingKeys = orderingKeys;
	    this.row = row;
	    this.descending = descd;
    }

	@Override
	public int compareTo(ComparableKeys o) {
		// TODO Auto-generated method stub
//		if (this.orderingKeys.size() != o.orderingKeys.size()) {
//			throw new RuntimeException("Bad ordering keys, there is a bug maybe");
//	    }
		
		String key = orderingKeys;
			KeyValueImpl a = this.row.get(key);
	        KeyValueImpl b = o.row.get(key);
	        
	        if (a == null || b == null) {
	          throw new RuntimeException("Bad input data: " + key);
	        }
	        
	        int ret = a.compareTo(b);
	        if(descending) {
	        	ret = -ret;
	        }
	        if (ret != 0) {
	          return ret;
	        }
	    
	    return 0;
	}
}
