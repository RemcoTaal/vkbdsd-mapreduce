package org.apache.hadoop.examples;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Test {

    public static void main(String[] args) {
        Text newValue = new Text("rabarber");
        StringTokenizer itr = new StringTokenizer(newValue.toString());
        ArrayList<Character> allChars = new ArrayList<>();
        ArrayList<MatrixLine> matrixLines = new ArrayList<>();

        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            for (int i = 0; i < word.length(); i++) {
                char a = word.charAt(i);

                if (!allChars.contains(a)) {
                    allChars.add(a);
                }

            }
        }


        ArrayList<Combination> allCombinations = getAllPossibleCombinations(allChars);
        AtomicInteger totalCount = new AtomicInteger();
        AtomicInteger index = new AtomicInteger();
        AtomicReference<Character> nextStartingChar = new AtomicReference<>((char) 0);
        allCombinations.forEach(combination -> {

            MatrixLine matrixLine = new MatrixLine();
            matrixLine.addCombination(combination);

            index.addAndGet(1);
            AtomicReference<Character> startingChar = new AtomicReference<>((char) combination.toString().charAt(0));


            if (index.intValue() < allCombinations.size() - 1) {
                nextStartingChar.set(allCombinations.get(index.intValue()).toString().charAt(0));
            }


            if (!startingChar.equals(nextStartingChar.get())) {

                int count = StringUtils.countMatches(newValue.toString(), combination.getCombination());
                combination.setCount(count);
//                System.out.println(combination + " " + count + "totalCount for char: " + startingChar + " = " + totalCount);
                totalCount.set(0);
                matrixLine.setTotal(totalCount.intValue());
                matrixLines.add(matrixLine);
            } else {
                int count = StringUtils.countMatches(newValue.toString(), combination.getCombination());
                totalCount.addAndGet(count);
                matrixLine.setTotal(totalCount.intValue());
//                System.out.println(combination + " " + count + "totalCount for char: " + startingChar + " = " + totalCount);
            }

        });

        matrixLines.forEach(matrixLine1 -> {
            System.out.println(matrixLine1.toString());
        });
    }

    public static ArrayList<Combination> getAllPossibleCombinations(ArrayList<Character> allChars) {
        ArrayList<Combination> possibleCombinations = new ArrayList<>();

        allChars.forEach(c1 -> {

            allChars.forEach(c2 -> {
                possibleCombinations.add(new Combination(c1.toString().toLowerCase() + c2.toString().toLowerCase()));
            });

        });

        return possibleCombinations;
    }

    public static class MatrixLine {
        ArrayList<Combination> combinations;
        int total = 0;

        public MatrixLine() {
            this.combinations = new ArrayList<Combination>();
        }

        public ArrayList<Combination> getCombination() {
            return combinations;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }

        public void addCombination(Combination combination) {
            this.combinations.add(combination);
        }

        @Override
        public String toString() {
            return "MatrixLine{" +
                    "combinations=" + combinations +
                    ", total=" + total +
                    '}';
        }
    }

    public static class Combination {
        String combination;
        int count = 0;

        Combination(String combination) {
            this.combination = combination;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        public String getCombination() {
            return combination;
        }

        @Override
        public String toString() {
            return "Combination{" +
                    "combination='" + combination + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


}
