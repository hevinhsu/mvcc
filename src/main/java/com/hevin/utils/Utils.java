package com.hevin.utils;

import java.util.Arrays;
import java.util.Objects;

public final class Utils {


	private static final boolean DEBUG = false;

	public static void assertWith(boolean pass, String errorMessage) {
		if (!pass) {
			throw new RuntimeException(errorMessage);
		}
	}

	public static <T> void assertEquals(T a, T b, String compareTarget) {
		if (Objects.equals(a, b)) {
			throw new RuntimeException(compareTarget + ": " + a + " != " + b);
		}
	}

	public static void debug(Object... a) {
		if (!DEBUG) {
			return;
		}
		Object[] args = new Object[a.length + 1];
		args[0] = "[DEBUG]";
		System.arraycopy(a, 0, args, 1, a.length);
		System.out.println(Arrays.toString(args));
	}
}
