package Index.BPlusTree;

// DO NOT change this file.

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        // if T is integer
        if(Integer.class.equals(typeClass)){
            return (T) (Integer) (((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF));
        }
        // if T is string
        else if(String.class.equals(typeClass)){
            return (T) new String(bytes);
        }
        // if T is double
        else if(Double.class.equals(typeClass)){
            long l = ((long) (bytes[0] & 0xFF) << 56) | ((long) (bytes[1] & 0xFF) << 48) | ((long) (bytes[2] & 0xFF) << 40) | ((long) (bytes[3] & 0xFF) << 32) | ((long) (bytes[4] & 0xFF) << 24) | ((long) (bytes[5] & 0xFF) << 16) | ((long) (bytes[6] & 0xFF) << 8) | (bytes[7] & 0xFF);
            return (T) (Double) Double.longBitsToDouble(l);
        }
        // if T is float
        else if(Float.class.equals(typeClass)){
            int i = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
            return (T) (Float) Float.intBitsToFloat(i);
        }
        // if T is boolean
        else if(Boolean.class.equals(typeClass)){
            return (T) (Boolean) (bytes[0] == 1);
        }
        return null;
    }
    
}