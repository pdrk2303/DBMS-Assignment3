
package index.bplusTree;
import java.util.*;
import java.util.Random;

public class main {
    public static void main(String[] args){
        try {
            int[] arr1 = new int[256];
            for (int i = 0; i < 256; i++){
                arr1[i] = i;
            }
            shuffleArray(arr1);
            System.out.println("bi\n ");
            BPlusTreeIndexFile<Integer> bPT = new BPlusTreeIndexFile<>(3, Integer.class);
            for(int i = 1; i <= 40; i++){
                bPT.insert(20, i+50);
            }
            for(int i = 1; i < 40; i++){
                bPT.insert(40-i, i+100);
            }

//            for (int i=1; i<=6; i++) {
//                bPT.insert(20, i);
//            }
//            for(int i=1; i<5; i++) {
//                bPT.insert(20+i, i+5);
//            }

//            for (int i = 0; i < 256; i++){
            System.out.println(20 + ": " + bPT.search(2));
            System.out.println("\n");
            System.out.println(13 + ": " + bPT.search(3));
            System.out.println(15 + ": " + bPT.search(4));
            System.out.println("\n");


//            }
            bPT.print();
            System.out.println();
            System.out.println("Printed\n");

            bPT.return_bfs();
            ArrayList<Integer> result = bPT.return_bfs();

            System.out.println("Printing BFS:");
            for(int i=0; i < result.size(); i++) {
                System.out.print(result.get(i) + " ");
            }
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void shuffleArray(int[] arr) {
        Random rnd = new Random();
        for (int i = arr.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Swap arr[i] and arr[index]
            int temp = arr[i];
            arr[i] = arr[index];
            arr[index] = temp;
        }
    }
}
