package simpledb;

import org.junit.Assert;
import org.junit.Test;
import simpledb.Predicate.Op;

import java.lang.reflect.Array;
import java.util.*;

import static java.lang.Integer.min;

public class StringHistogramTest {

	public static String generateRandomChars(String candidateChars, int length) {
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		int string_length = random.nextInt(length);
		for (int i = 0; i < string_length; i++) {
			sb.append(candidateChars.charAt(random.nextInt(candidateChars
					.length())));
		}
		return sb.toString();
	}

	public static void printAllKLength(char[] set, int k, ArrayList<String> res) {
		int n = set.length;
		printAllKLengthRec(set, "", n, k, res);
	}

	public static void printAllKLengthRec(char[] set, String prefix, int n, int k, ArrayList<String> res) {

		if (k == 0) {
			res.add(prefix);
			return;
		}

		for (int i = 0; i < n; ++i) {
			String newPrefix = prefix + set[i];
			printAllKLengthRec(set, newPrefix, n, k - 1, res);
		}
	}


	/**
	 * Test to confirm that the StringHistogram implementation is constant-space
	 * (or, at least, reasonably small space; O(log(n)) might still work if
	 * your constants are good).
	 */
	@Test public void opLikeTest() {
		// Don't bother with a timeout on this test.
		// Printing debugging statements takes >> time than some inefficient algorithms.
		SuffixTreeStringHistogram h = new SuffixTreeStringHistogram();
		String alphaNumericChars = "abcdefghijklmnopqrstuvwxyz";

		// Feed the histogram more alphabetic string than would fit into our
		// 128mb allocated heap (4-byte string)
		// If this fails, someone's storing every value...
		// "a" string fields
		for (int c = 0; c < 16554432; c++) { // should be 134 MB
			String uuid = "a" + generateRandomChars(alphaNumericChars.substring(1), 4);
			h.addValue(uuid);	// Pseudo-random number; at least get a distribution
		}

		// No-"a" string fields
		for (int c = 0; c < 16554432; c++) {
			String uuid = generateRandomChars(alphaNumericChars.substring(1), 5);
			h.addValue(uuid);	// Pseudo-random number; at least get a distribution
		}

		Assert.assertTrue(h.estimateSelectivity(Op.LIKE, "a") == 0.5); // LIKE *a* -> 0.5
	}

	@Test public void opLikeTestWithoutSuffixTree() {
		// Don't bother with a timeout on this test.
		// Printing debugging statements takes >> time than some inefficient algorithms.
		StringHistogram h = new StringHistogram(1000000);
		String alphaNumericChars = "abcdefghijklmnopqrstuvwxyz";

		// Feed the histogram more alphabetic string than would fit into our
		// 128mb allocated heap (4-byte string)
		// If this fails, someone's storing every value...
		// "a" string fields
		for (int c = 0; c < 16554432; c++) { // should be 134 MB
			String uuid = "a" + generateRandomChars(alphaNumericChars.substring(1), 4);
			h.addValue(uuid);	// Pseudo-random number; at least get a distribution
		}

		// No-"a" string fields
		for (int c = 0; c < 16554432; c++) {
			String uuid = generateRandomChars(alphaNumericChars.substring(1), 5);
			h.addValue(uuid);	// Pseudo-random number; at least get a distribution
		}

		double selectivity = h.estimateSelectivity(Op.LIKE, "a");
		Debug.log("selectivity = %.2f", selectivity);
		Assert.assertTrue(selectivity < 0.1); // Answer should be 0.5
	}



	@Test public void suffixTreeTest() {
		SuffixTree tree = new SuffixTree("greener");

		for (int i = 0; i < 100; i++)
			tree.insertString("grey");
		for (int i = 0; i < 99; i++)
			tree.insertString("greener");
		for (int i = 0; i < 100; i++)
			tree.insertString("grew");
		Debug.log("%s", tree.printTree());

		Assert.assertTrue(tree.searchText("gre") == 300.0);
		Assert.assertTrue(tree.searchText("ey") == 100.0);
		Assert.assertTrue(tree.searchText("n") == 100.0);
		Assert.assertTrue(tree.searchText("g") == 300.0);
		Assert.assertTrue(tree.searchText("") == 300.0);

	}
}
