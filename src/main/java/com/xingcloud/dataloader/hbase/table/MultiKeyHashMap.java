package com.xingcloud.dataloader.hbase.table;

import java.util.*;

/**
 *
 * Author: mulisen
 * Date:   12-8-2
 */
public class MultiKeyHashMap<T> implements MultiKeyMap<T>{

	public static final String DEFAULT_DELIMITER = ".";

	private Node<T> root = new Node<T>(null, null);

	private String delimiter = DEFAULT_DELIMITER;

	int size = 0;

	public MultiKeyHashMap() {
		this(DEFAULT_DELIMITER);
	}

	public MultiKeyHashMap(String delimiter) {
		this.delimiter = delimiter;
	}

	public T put(T value, String... key) {
		if(value == null){
			throw new NullPointerException();
		}
		key = normalize(key);
		Node<T> cursor = root;
		for (int i = 0; i < key.length; i++) {
			String s = key[i];
//			if(cursor.children == null){
//				cursor.children = new HashMap<String, Node<T>>();
//			}
			Node<T> next = cursor.children.get(s);
			if(next == null){
				next = new Node<T>(Arrays.copyOf(key,i+1), cursor);
				cursor.children.put(s, next);
			}
			cursor = next;
		}
		T oldValue = cursor.value;
		cursor.value = value;
		if(oldValue == null){
			size++;
		}
		return oldValue;
	}

	public T get(String... key) {
		Node<T> cursor = getNode(normalize(key));
		return cursor == null? null:cursor.value;
	}

	private Node<T> getNode(String ... key){
		Node<T> cursor = root;
		for (int i = 0; i < key.length; i++) {
			String s = key[i];
//			if(cursor.children == null){
//				return null;
//			}
			Node<T> next = cursor.children.get(s);
			if(next == null){
				return null;
			}
			cursor = next;
		}
		return cursor;
	}


	public Set<String> dir(String... key) {
		key = normalize(key);
		Node<T> cursor = root;
		for (int i = 0; i < key.length; i++) {
			String s = key[i];
//			if(cursor.children == null){
//				return new HashSet<String>();
//			}
			Node<T> next = cursor.children.get(s);
			if(next == null){
				return new HashSet<String>();
			}
			cursor = next;
		}
//		if(cursor.children == null){
//			return new HashSet<String>();
//		}
		return cursor.children.keySet();
	}

	public T remove(String... key) {
		key = normalize(key);
		Node<T> cursor = root;
		for (int i = 0; i < key.length; i++) {
			String s = key[i];
			Node<T> next = cursor.children.get(s);
			if(next == null){
				return null;
			}
			cursor = next;
		}
		T oldValue = cursor.value;
		cursor.value = null;
		for(Node<T> parent = cursor; parent != null; parent = parent.parent){
			if(parent == null || parent.children.size()!=0 || parent == root){
				break;
			}
			if(parent.children.size()==0){
				parent.parent.children.remove(parent.key[parent.key.length-1]);
			}
		}
		if(oldValue != null){
			size --;
		}
		return oldValue;
	}

	public String delimiter() {
		return this.delimiter;
	}

	public void delimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public int size() {
		return this.size;
	}

	private String[] normalize(String[] key) {
		if(key == null || key.length != 1) return key;
		StringTokenizer st = new StringTokenizer(key[0], delimiter);
		if(!key[0].contains(delimiter)) return key;
		List<String> newkey = new ArrayList<String>();
		while(st.hasMoreTokens()){
			newkey.add(st.nextToken());
		}
		return newkey.toArray(new String[0]);
	}

	public Iterator<T> iterator() {
		return root.iterator();
	}

	public Iterator<T> iterator(String ... key){
		key = normalize(key);
		Node<T> node = getNode(key);
		return node == null? new Node(new String[0], null).iterator():node.iterator();
	}

	static class Node<TT> implements Iterable<TT>{
		Map<String, Node<TT>> children = new HashMap<String, Node<TT>>();
		TT value = null;
		String[] key;
		Node<TT> parent;

		Node(String[] key, Node<TT> parent) {
			this.key = key;
			this.parent = parent;
		}

		public NodeIterator<TT> iterator() {
			return new NodeIterator<TT>(this);
		}
	}

	private static class NodeIterator<TT> implements Iterator<TT> {

		Node<TT> current;
		Node[] children;
		NodeIterator<TT> currentChildIterator;
		int currentChildIndex = -1;
		boolean thisVisited = false;
		public NodeIterator(Node<TT> current) {
			this.current = current;
			if(current != null && current.children != null){
				children = current.children.values().toArray(new Node[0]);
				if(children.length>0){
					currentChildIterator=children[0].iterator();
					currentChildIndex = 0;
				}
			}
		}

		public boolean hasNext() {
			seekNext();
			if(currentChildIterator == null){
				if(!thisVisited && current.value != null){
					return true;
				}else{
					return false;
				}
			}else{
				return currentChildIterator.hasNext();
			}
		}

		private void seekNext() {
			if(currentChildIterator == null || currentChildIterator.hasNext()){
				return;
			}
			for(currentChildIndex++;currentChildIndex<children.length;currentChildIndex++){
				currentChildIterator = children[currentChildIndex].iterator();
				if(currentChildIterator.hasNext()){
					break;
				}
			}
			if(currentChildIndex == children.length){
				currentChildIterator = null;
			}

		}

		public TT next() {
			seekNext();
			if(currentChildIterator == null){
				if(!thisVisited  && current.value != null){
					thisVisited = true;
					return current.value;
				}else{
					thisVisited = true;
					return null;
				}
			}else{
				return currentChildIterator.next();
			}
		}

		public void remove() {
			//do not support
		}
	}


	public static void main(String[] args) {
		test();
	}

	private static void test() {
		MultiKeyHashMap<Integer> map = new MultiKeyHashMap<Integer>();

		map.put(7, "www.net");
		map.put(8, "www.com.xc");
//        map.put(5, "www.com");
//        map.put(6, "www", "com");

        System.out.println(map.get("www.com.xc"));
        System.out.println(map.get("www.com"));

		map.put(9, "www.com.dvdv.rertt");
		
		map.put(10, "www.com.xc.ibm.test.good");

		map.remove("www");
		System.out.println("map = " + map.get("www.com"));
		System.out.println("map = " + map.get("www", "com"));
		System.out.println("map = " + map.get("www", "net"));
		System.out.println("map = " + map.get("www", "net9"));
		System.out.println("map = " + map.dir("www"));
		System.out.println("map = " + map.dir("www.com"));
		map.remove("www.com.dvdv.rertt");
		System.out.println("map = " + map.dir("www", "com"));
		System.out.println("map = " + map.dir("www.com.test"));
		System.out.println("map = " + map.dir(""));
		for(Integer i:map){
			System.out.println("i = " + i);
		}

	}

}
