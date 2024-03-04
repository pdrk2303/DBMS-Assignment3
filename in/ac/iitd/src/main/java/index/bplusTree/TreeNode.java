package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key); 
    // returns the block id where the key is present
    // for InternalNode, it will return the block id of the child node
    // for LeafNode, it will return the block id of the record


    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */

        return null;
    }
    
}