package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */

        if (bytes != null && bytes.length > 0) {
            if (typeClass == Integer.class) {
                int val = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
                return typeClass.cast(val);
            } else if (typeClass == Boolean.class) {
                boolean val = bytes[0] != 0;
                return typeClass.cast(val);
            } else if (typeClass == Float.class) {
                int val = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
                return typeClass.cast(Float.intBitsToFloat(val));
            } else if (typeClass == Double.class) {
                long val = ((bytes[0] & 0xFFL) << 56) | ((bytes[1] & 0xFFL) << 48) | ((bytes[2] & 0xFFL) << 40) |
                        ((bytes[3] & 0xFFL) << 32) | ((bytes[4] & 0xFFL) << 24) | ((bytes[5] & 0xFFL) << 16) |
                        ((bytes[6] & 0xFFL) << 8) | (bytes[7] & 0xFFL);
                return typeClass.cast(Double.longBitsToDouble(val));
            } else if (typeClass == String.class) {
                return typeClass.cast(new String(bytes));
            }
        }

        return null;
    }

    default public byte[] TtoBytes_conversion (T t) {

        if (t != null) {
            if (t instanceof Integer) {
                int val = (Integer) t;
                byte[] bytes = new byte[4];
                bytes[0] = (byte) (val >> 24);
                bytes[1] = (byte) (val >> 16);
                bytes[2] = (byte) (val >> 8);
                bytes[3] = (byte) (val);

                return bytes;

            } else if (t instanceof Boolean) {
                byte val = (byte) (((Boolean) t) ? 1 : 0);
                return new byte[]{val};

            } else if (t instanceof Float) {
                int val = Float.floatToIntBits((Float) t);
                byte[] bytes = new byte[4];
                bytes[0] = (byte) (val >> 24);
                bytes[1] = (byte) (val >> 16);
                bytes[2] = (byte) (val >> 8);
                bytes[3] = (byte) (val);

                return bytes;

            } else if (t instanceof Double) {
                long val = Double.doubleToLongBits((Double) t);
                byte[] bytes = new byte[8];
                bytes[0] = (byte) (val >> 56);
                bytes[1] = (byte) (val >> 48);
                bytes[2] = (byte) (val >> 40);
                bytes[3] = (byte) (val >> 32);
                bytes[4] = (byte) (val >> 24);
                bytes[5] = (byte) (val >> 16);
                bytes[6] = (byte) (val >> 8);
                bytes[7] = (byte) (val);

                return bytes;

            } else if (t instanceof String) {
                return ((String) t).getBytes();

            } else {
                throw new IllegalArgumentException("Unsupported typeclass for conversion");
            }
        }

        return null;
    }

    
}