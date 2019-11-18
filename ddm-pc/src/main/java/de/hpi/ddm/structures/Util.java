package de.hpi.ddm.structures;

import scala.Char;

import java.util.Arrays;

public class Util {

    // Function to swap the data
    // present in the left and right indices
    public static Character[] swap(Character data[], int left, int right)
    {

        // Swap the data
        char temp = data[left];
        data[left] = data[right];
        data[right] = temp;

        // Return the updated array
        return data;
    }

    // Function to reverse the sub-array
    // starting from left to the right
    // both inclusive
    public static Character[] reverse(Character data[], int left, int right)
    {

        // Reverse the sub-array
        while (left < right) {
            char temp = data[left];
            data[left++] = data[right];
            data[right--] = temp;
        }

        // Return the updated array
        return data;
    }

    // Function to find the next permutation
    // of the given integer array
    public static boolean findNextPermutation(Character data[])
    {

        // If the given dataset is empty
        // or contains only one element
        // next_permutation is not possible
        if (data.length <= 1)
            return false;

        int last = data.length - 2;

        // find the longest non-increasing suffix
        // and find the pivot
        while (last >= 0) {
            if (data[last] < data[last + 1]) {
                break;
            }
            last--;
        }

        // If there is no increasing pair
        // there is no higher order permutation
        if (last < 0)
            return false;

        int nextGreater = data.length - 1;

        // Find the rightmost successor to the pivot
        for (int i = data.length - 1; i > last; i--) {
            if (data[i] > data[last]) {
                nextGreater = i;
                break;
            }
        }

        // Swap the successor and the pivot
        data = swap(data, nextGreater, last);

        // Reverse the suffix
        data = reverse(data, last + 1, data.length - 1);

        // Return true as the next_permutation is done
        return true;
    }

    // Driver Code
    /*
    public static void main(String args[])
    {
        Character beginData[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'};
        Character permutatedData[] = new Character[beginData.length];
        Character searched[] = {'a', 'b', 'c', 'e', 'd', 'f', 'g', 'h', 'i', 'j', 'k'};
        System.arraycopy(beginData, 0, permutatedData, 0, beginData.length);
        if (Arrays.equals(beginData, searched)) {
            System.out.println("Fertig nach 1.");
        }
        else {
            while (!beginData.equals(permutatedData)) {
                if (!findNextPermutation(permutatedData))
                    break;
                else {
                    if (Arrays.equals(permutatedData, searched)) {
                        System.out.println(Arrays.toString(permutatedData));
                        break;
                    }
                }
            }
        }
    }
    */

}
