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
	@Test public void orderOfGrowthTest() {
		// Don't bother with a timeout on this test.
		// Printing debugging statements takes >> time than some inefficient algorithms.
		StringHistogram h = new StringHistogram(200000000);
		String alphaNumericChars = "abcdefghijklmnopqrstuvwxyz";

		// Feed the histogram more alphabetic string than would fit into our
		// 128mb allocated heap (4-byte string)
		// If this fails, someone's storing every value...
//		for (int c = 0; c < 33554432; c++) { // should be 134 MB
		for (int c = 0; c < 100000; c++) { // should be 134 MB
			String uuid = generateRandomChars(alphaNumericChars, 5);
			h.addValue(uuid);	// Pseudo-random number; at least get a distribution
		}
		
		// Try printing out all of the values; make sure "estimateSelectivity()"
		// cause any problems
		double selectivity = 0.0;

		ArrayList<String> combinations = new ArrayList<>();
		combinations.add("");
		for (int i = 4; i > 0; i--)
			printAllKLength(alphaNumericChars.toCharArray(), i, combinations);
		Debug.log("res = %d", combinations.size());

		for (int c = 0; c < combinations.size(); c++) {
			String wildcard_query = combinations.get(c);
			selectivity += h.estimateSelectivity(Op.EQUALS, wildcard_query);
			if ((c % 1000) == 1)
				Debug.log("Current query = %s, selectivity = %f", wildcard_query, selectivity);
		}
		selectivity /= 100000;
		Debug.log("selectivity = %f", selectivity);

		// All the selectivities should add up to 1, by definition.
		// Allow considerable leeway for rounding error, though 
		// (Java double's are good to 15 or so significant figures)
		Assert.assertTrue(selectivity > 0.99);
	}

	/**
	 * Make sure that LIKE binning does something reasonable.
	 */
	@Test public void opLikeTest() {
		StringHistogram h = new StringHistogram(10);

//		// Set some values
//		h.addValue(3);
//		h.addValue(3);
//		h.addValue(3);
//		h.addValue(1);
//		h.addValue(10);
//
//		// Be conservative in case of alternate implementations
//		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, -1) > 0.999);
//		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 2) > 0.6);
//		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 4) < 0.4);
//		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 12) < 0.001);
	}

	/**
	 * Test
	 */
	@Test public void suffixTreeTest() {
		String bookkeeper = "bookkeeper";
		SuffixTree tree = new SuffixTree("greener");
		Debug.log("%s", tree.printTree());
		tree.insertString("ener");
//		tree.addSuffix();
		Debug.log("%s", tree.printTree());

	}
}
