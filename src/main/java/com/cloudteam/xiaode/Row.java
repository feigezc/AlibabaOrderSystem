package com.cloudteam.xiaode;

import java.util.HashMap;


@SuppressWarnings("serial")
public class Row extends HashMap<String, KeyValueImpl> {
	
	public Row() {
		super();
	}
	
	public Row(KeyValueImpl kv) {
		super();
		this.put(kv.key(), kv);
	}
	
	public KeyValueImpl getKV(String key) {
		KeyValueImpl kv = this.get(key);
		
		if (kv == null) {
			throw new RuntimeException(key + " is not exist");
		}
		return kv;
	}
	
	public Row putKV(String key, String value) {
		KeyValueImpl kv = new KeyValueImpl(key, value);
		this.put(kv.key(), kv);
		return this;
	}

    public Row putKV(String key, long value) {
	    KeyValueImpl kv = new KeyValueImpl(key, Long.toString(value));
	    this.put(kv.key(), kv);
	    return this;
	}
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{\n");
      for (KeyValueImpl kv : this.values()) {
          sb.append(kv.toString());
          sb.append(",\n");
        }
      sb.append('}');
      return sb.toString();
    }
}
