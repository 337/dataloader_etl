package com.xingcloud.dataloader.hbase.table;

import java.util.Iterator;
import java.util.Set;

/**
 *
 * 多键对应一个值的hash表.
 *
 * mk.put(0, "www.xingcloud");
 * mk.put(1, "www.xingcloud.com");
 * mk.put(2, "www", "xingcloud", "com"); //和上一条等价
 *
 * 将得到：
 *
 * mk.get("www") => null;
 * mk.get("www.xingcloud") => 0;
 * mk.get("www.xingcloud.com") => 2;
 * mk.dir("www") => {"xingcloud"};
 * mk.dir("www.xingcloud") => {"com"};
 * mk.dir("www", "xingcloud") => {"com"};
 *
 * Author: mulisen
 * Date:   12-8-2
 */
public interface MultiKeyMap<T> extends Iterable<T>{

	public T put(T value, String ... key);

	public T get(String ... key);

	public Set<String> dir(String ... key);

	public T remove(String ... key);

	public String delimiter();

	public void delimiter(String delimiter);

	public int size();

	public Iterator<T> iterator(String ... key);
}
