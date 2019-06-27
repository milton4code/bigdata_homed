package com.examples;

import org.apache.log4j.Logger;

import java.io.OutputStream;
import java.io.PrintStream;

public class Log4jPrintStream extends PrintStream {
	private Logger log = Logger.getLogger("SystemOut");
	private static PrintStream instance = new Log4jPrintStream(System.out);

	private Log4jPrintStream(OutputStream out) {
		super(out);
	}

	public static void redirectSystemOut() {
		System.setOut(instance);
	}

	public void print(boolean b) {
		println(b);
	}

	public void print(char c) {
		println(c);
	}

	public void print(char[] s) {
		println(s);
	}

	public void print(double d) {
		println(d);
	}

	public void print(float f) {
		println(f);
	}

	public void print(int i) {
		println(i);
	}

	public void print(long l) {
		println(l);
	}

	public void print(Object obj) {
		println(obj);
	}

	public void print(String s) {
		println(s);
	}

	public void println(boolean x) {
		log.error(Boolean.valueOf(x));
	}

	public void println(char x) {
		log.error(Character.valueOf(x));
	}

	public void println(char[] x) {
		log.error(x == null ? null : new String(x));
	}

	public void println(double x) {
		log.error(Double.valueOf(x));
	}

	public void println(float x) {
		log.error(Float.valueOf(x));
	}

	public void println(int x) {
		log.error(Integer.valueOf(x));
	}

	public void println(long x) {
		log.error(x);
	}

	public void println(Object x) {
		log.error(x);
	}

	public void println(String x) {
		log.error(x);
	}

}
/*
 *
 * public void println(boolean x) {
 * if(log.isDebugEnabled()) {
 * log.error(Boolean.valueOf(x));
 * }
 * }
 */