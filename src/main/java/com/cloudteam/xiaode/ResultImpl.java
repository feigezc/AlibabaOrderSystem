package com.cloudteam.xiaode;

import java.util.Set;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;

public class ResultImpl implements Result {

	private long orderid;
    private Row kvMap;
    
    
	public ResultImpl(long orderid, Row kv) {
		this.orderid = orderid;
        this.kvMap = kv;
	}

	@Override
	public KeyValueImpl get(String key) {
		return this.kvMap.get(key);
	}

	@Override
	public KeyValueImpl[] getAll() {
		// TODO Auto-generated method stub
		return kvMap.values().toArray(new KeyValueImpl[0]);
	}

	@Override
	public long orderId() {
		// TODO Auto-generated method stub
		return orderid;
	}

	public static ResultImpl createResultRow(Row orderData, Row buyerData,
        Row goodData, Set<String> queryingKeys) {
    	
      if (orderData == null || buyerData == null || goodData == null) {
        throw new RuntimeException("Bad data!");
      }
      Row allkv = new Row();
      long orderid;
      try {
        orderid = orderData.get("orderid").valueAsLong();
      } catch (TypeException e) {
        throw new RuntimeException("Bad data!");
      }

      for (KeyValueImpl kv : orderData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KeyValueImpl kv : buyerData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KeyValueImpl kv : goodData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      return new ResultImpl(orderid, allkv);
    }


    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("orderid: " + orderid + " {");
      if (kvMap != null && !kvMap.isEmpty()) {
        for (KeyValueImpl kv : kvMap.values()) {
          sb.append(kv.toString());
          sb.append(",\n");
        }
      }
      sb.append('}');
      return sb.toString();
    }

}
