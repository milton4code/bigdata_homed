package com.examples;

public class SystemOut2Log {
	static {
		Log4jPrintStream.redirectSystemOut();
	}

	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) {
			System.out.print("abc");
			System.out.print(i);
			System.out.print((int)(Math.random()*100));
		}
	}
}