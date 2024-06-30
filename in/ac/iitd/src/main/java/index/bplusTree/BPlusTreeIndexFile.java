package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;

/*
 * Tree is a collection of BlockNodes
 * The first BlockNode is the metadata block - stores the order and the block_id of the root node

 * The total number of keys in all leaf nodes is the total number of records in the records file.
 */

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    private boolean compare_greaterThan(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return (Integer) k1 > (Integer) k2;
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) > ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return (Float) k1 > (Float) k2;
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) > 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return (Double) k1 > (Double) k2;
        } else {
            return false;
        }
    }

    private boolean compare_equal(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return ((Integer) k1).equals((Integer) k2);
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) == ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return ((Float) k1).equals((Float) k2);
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) == 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return ((Double) k1).equals((Double) k2);
        } else {
            return false;
        }
    }

    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */

        if (this == null) {
            return;
        }

        if (key == null) {
            return;
        }

        int rootId = getRootId();
        if (isLeaf(rootId)) {
            LeafNode<T> leafNode = (LeafNode<T>) blocks.get(rootId);

            if (isFull(rootId)) {
                int m = getOrder();

                T[] keys = leafNode.getKeys();
                int[] blockIds = leafNode.getBlockIds();


                int i = 0;
                int flag = 0;
                while(i < keys.length) {
                    if (compare_equal(key, keys[i])) {
                        flag = 1;
                        break;
//                        i++;
                    } else if (compare_greaterThan(key, keys[i])) {
                        i++;
                    } else {
                        break;
                    }
                }

//                if (flag == 1) {
//                    int start = i;
//                    int end = i;
//                    while (end < keys.length) {
//                        if (compare_equal(key, keys[end])) {
//                            end++;
//                        } else {
//                            break;
//                        }
//                    }
//                    end--;
//                    while (start <= end) {
//                        int temp = blockIds[start];
//                        blockIds[start] = blockIds[end];
//                        blockIds[end] = temp;
//
//                        start++;
//                        end--;
//                    }
//
//                }


                T[] new_keys = (T[]) new Object[m];
                int[] new_blockIds = new int[m];
                int j = 0;
                while(j < i) {
                    new_keys[j] = keys[j];
                    new_blockIds[j] = blockIds[j];
                    j++;
                }
                new_keys[j] = key;
                new_blockIds[j] = block_id;
                j++;
                while(j < m) {
                    new_keys[j] = keys[j-1];
                    new_blockIds[j] = blockIds[j-1];
                    j++;
                }


                byte[] num_keys = new byte[2];
                num_keys[0] = (byte) (0);
                num_keys[1] = (byte) (0);

                byte[] next_free_offset_bytes = new byte[2];
                next_free_offset_bytes[0] = 0;
                next_free_offset_bytes[1] = 8;

                leafNode.write_data(0, num_keys);
                leafNode.write_data(6, next_free_offset_bytes);

                int k = 0;

                while (k < m/2) {
                    leafNode.insert(new_keys[k], new_blockIds[k]);
                    k++;
                }


                key = new_keys[k];
                LeafNode<T> right_leafNode = new LeafNode<>(this.typeClass);
                while(k < m) {
                    right_leafNode.insert(new_keys[k], new_blockIds[k]);
                    k++;
                }



                blocks.add(right_leafNode);
                blocks.set(blocks.size()-1, right_leafNode);

                block_id = blocks.size()-1;


                InternalNode<T> new_root = new InternalNode<>(key, rootId, block_id, this.typeClass);
                blocks.add(new_root);
                blocks.set(blocks.size()-1, new_root);

                int x = blocks.size()-1;

                int new_rootId = blocks.size()-1;
                BlockNode first_block = blocks.get(0);
                byte[] new_rootId_bytes = new byte[2];
                new_rootId_bytes[0] = (byte) ((new_rootId >> 8) & 0xFF);
                new_rootId_bytes[1] = (byte) (new_rootId & 0xFF);
                first_block.write_data(2, new_rootId_bytes);

            } else {
                leafNode.insert(key, block_id);
            }
        } else {

            Stack<Integer> stack = new Stack<>();

            while (!isLeaf(rootId)) {
                stack.push(rootId);
                InternalNode<T> internalNode = (InternalNode<T>) blocks.get(rootId);
                T[] keys = internalNode.getKeys();
                int[] children = internalNode.getChildren();


                int i = 0;
                while(i < keys.length) {
                    if (compare_equal(key, keys[i])) {
                        break;
//                        i++;
                    } else if(compare_greaterThan(key, keys[i])) {
                        i++;
                    } else {
                        break;
                    }
                }

                rootId = children[i];
            }


            LeafNode<T> leafNode = (LeafNode<T>) blocks.get(rootId);
            if (isFull(rootId)) {
                int m = getOrder();
                T[] keys = leafNode.getKeys();
                int[] blockIds = leafNode.getBlockIds();


                int i = 0;
                int flag = 0;
                while(i < keys.length) {
                    if (compare_equal(key, keys[i])) {
                        flag = 1;
                        break;
//                        i++;
                    } else if (compare_greaterThan(key, keys[i])) {
                        i++;
                    } else {
                        break;
                    }
                }

//                if (flag == 1) {
//                    int start = i;
//                    int end = i;
//                    while (end < keys.length) {
//                        if (compare_equal(key, keys[end])) {
//                            end++;
//                        } else {
//                            break;
//                        }
//                    }
//                    end--;
//                    while (start <= end) {
//                        int temp = blockIds[start];
//                        blockIds[start] = blockIds[end];
//                        blockIds[end] = temp;
//
//                        start++;
//                        end--;
//                    }
//
//                }

                T[] new_keys = (T[]) new Object[m];
                int[] new_blockIds = new int[m];

                int j = 0;
                while (j < i) {
                    new_keys[j] = keys[j];
                    new_blockIds[j] = blockIds[j];
                    j++;
                }

                new_keys[j] = key;
                new_blockIds[j] = block_id;
                j++;

                while (j < m) {
                    new_keys[j] = keys[j-1];
                    new_blockIds[j] = blockIds[j-1];
                    j++;
                }


                byte[] num_keys = new byte[2];
                num_keys[0] = (byte) (0);
                num_keys[1] = (byte) (0);

                byte[] next_free_offset_bytes = new byte[2];
                next_free_offset_bytes[0] = 0;
                next_free_offset_bytes[1] = 8;

                leafNode.write_data(0, num_keys);
                leafNode.write_data(6, next_free_offset_bytes);

                int k = 0;
                while (k < m/2) {
                    leafNode.insert(new_keys[k], new_blockIds[k]);
                    k++;
                }


                key = new_keys[k];
                LeafNode<T> right_leafNode = new LeafNode<>(this.typeClass);
                while (k < m) {
                    right_leafNode.insert(new_keys[k], new_blockIds[k]);
                    k++;
                }


                blocks.add(right_leafNode);
                blocks.set(blocks.size()-1, right_leafNode);

                block_id = blocks.size()-1;

                while (!stack.isEmpty()) {
                    int id = stack.pop();
                    InternalNode<T> node = (InternalNode<T>) blocks.get(id);

                    if (isFull(id)) {
                        T[] node_keys = node.getKeys();
                        int[] node_children = node.getChildren();


                        int a = 0;
                        int flag1= 0;
                        while(a < node_keys.length) {
                            if (compare_equal(key, node_keys[a])) {
                                flag1 = 1;
                                break;
//                                a++;
                            } else if (compare_greaterThan(key, node_keys[a])) {
                                a++;
                            } else {
                                break;
                            }
                        }

//                        if (flag1 == 1) {
//                            int start = a;
//                            int end = a;
//                            while (end < node_keys.length) {
//                                if (compare_equal(key, node_keys[end])) {
//                                    end++;
//                                } else {
//                                    break;
//                                }
//                            }
////                            end--;
//                            while (start <= end) {
//                                int temp = node_children[start];
//                                node_children[start] = node_children[end];
//                                node_children[end] = temp;
//
//                                start++;
//                                end--;
//                            }
//
//                        }

                        T[] node_new_keys = (T[]) new Object[m];
                        int[] node_new_children = new int[m+1];

                        int b = 0;
                        while (b < a) {
                            node_new_keys[b] = node_keys[b];
                            b++;
                        }
                        node_new_keys[b] = key;
                        b++;
                        while (b < m) {
                            node_new_keys[b] = node_keys[b-1];
                            b++;
                        }

                        int c = 0;
                        while (c <= a) {
                            node_new_children[c] = node_children[c];
                            c++;
                        }
                        node_new_children[c] = block_id;
                        c++;
                        while (c <= m) {
                            node_new_children[c] = node_children[c-1];
                            c++;
                        }


                        byte[] node_num_keys = new byte[2];
                        node_num_keys[0] = (byte) (0);
                        node_num_keys[1] = (byte) (0);

                        byte[] nextFreeOffsetBytes = new byte[2];
                        nextFreeOffsetBytes[0] = 0;
                        nextFreeOffsetBytes[1] = 6;

                        byte[] id_child1 = new byte[2];
                        id_child1[0] = (byte) ((node_new_children[0] >> 8) & 0xFF);
                        id_child1[1] = (byte) (node_new_children[0] & 0xFF);

                        node.write_data(0, node_num_keys);
                        node.write_data(2, nextFreeOffsetBytes);
                        node.write_data(4, id_child1);

                        int d = 0;
                        while (d < m/2) {
                            node.insert(node_new_keys[d], node_new_children[d+1]);
                            d++;
                        }


                        key = node_new_keys[d];
                        d++;

                        InternalNode<T> right_node = new InternalNode<>(node_new_keys[d], node_new_children[d], node_new_children[d+1], this.typeClass);

                        d++;

                        while (d < m) {
                            right_node.insert(node_new_keys[d], node_new_children[d+1]);
                            d++;
                        }


                        blocks.add(right_node);
                        blocks.set(blocks.size()-1, right_node);

                        block_id = blocks.size()-1;

                        if (stack.isEmpty()) {
                            InternalNode<T> new_root_node = new InternalNode<>(key, id, block_id, this.typeClass);
                            blocks.add(new_root_node);
                            blocks.set(blocks.size()-1, new_root_node);
                            int new_rootId = blocks.size()-1;

                            BlockNode first_block = blocks.get(0);
                            byte[] new_rootId_bytes = new byte[2];
                            new_rootId_bytes[0] = (byte) ((new_rootId >> 8) & 0xFF);
                            new_rootId_bytes[1] = (byte) (new_rootId & 0xFF);
                            first_block.write_data(2, new_rootId_bytes);
                        }

                    } else {
                        node.insert(key, block_id);
                        break;
                    }
                }
            } else {
                leafNode.insert(key, block_id);
            }
        }

        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        if (this == null) {
            return -1;
        }
        if (key == null) {
            return -1;
        }

        int rootId = getRootId();
        int f = 1;

        while (!isLeaf(rootId)) {
            InternalNode<T> node = (InternalNode<T>) blocks.get(rootId);
            T[] keys = node.getKeys();
            int[] children = node.getChildren();
            int i=0;
            while(i < keys.length) {
                if (compare_equal(key, keys[i])) {
                    break;
                } else if(compare_greaterThan(key, keys[i])) {
                    i++;
                } else {
                    break;
                }
            }
            rootId = children[i];
            if (i != keys.length) {
                f = 0;
            }
        }

        LeafNode<T> leafNode = (LeafNode<T>) blocks.get(rootId);
        T[] leaf_keys = leafNode.getKeys();
        if (f==1 && compare_greaterThan(key, leaf_keys[leaf_keys.length-1])) {
            return -1;
        } else {
            return rootId;
        }
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}