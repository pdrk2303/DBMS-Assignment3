package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */

        int offset = 4;
        int i = 0;

        while(i < numKeys) {
            byte[] key_lengthBytes = this.get_data(offset+2, 2);
            int key_length = ((key_lengthBytes[0] & 0xFF) << 8) | (key_lengthBytes[1] & 0xFF);
            offset += 2;

            byte[] keyBytes = this.get_data(offset+2, key_length);
            offset += 2;

            keys[i] = convertBytesToT(keyBytes, this.typeClass);
            offset += key_length;
            i++;
        }

        return keys;

    }

    private int offsetToInsert(int end) {

        int offset = 6;
        int i = 0;

        while (i < end) {
            byte[] key_lengthBytes = this.get_data(offset, 2);
            int key_length = ((key_lengthBytes[0] & 0xFF) << 8) | (key_lengthBytes[1] & 0xFF);
            offset += 4 + key_length;
            i++;
        }

        return offset;
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

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {

        if (key == null) {
            return;
        }
        /* Write your code here */

        T[] keys = getKeys();
        int[] children = getChildren();

        int num_keys = getNumKeys();
        int i = 0;
        while (i < num_keys) {
            if (compare_equal(key, keys[i])) {
//                break;
                i++;
            } else if (compare_greaterThan(key, keys[i])) {
                i++;
            } else {
                break;
            }
        }

        int offset_to_insert = offsetToInsert(i);

        byte[] keyBytes = TtoBytes_conversion(key);
        byte[] key_length_bytes = new byte[2];
        key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);

        byte[] right_childId_bytes = new byte[2];
        right_childId_bytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        right_childId_bytes[1] = (byte) (right_block_id & 0xFF);

        this.write_data(offset_to_insert, key_length_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, keyBytes);
        offset_to_insert += keyBytes.length;
        this.write_data(offset_to_insert, right_childId_bytes);
        offset_to_insert += 2;

        while (i < num_keys) {
            byte[] key_bytes = TtoBytes_conversion(keys[i]);
            byte[] key_len_bytes = new byte[2];
            key_len_bytes[0] = (byte) ((key_bytes.length >> 8) & 0xFF);
            key_len_bytes[1] = (byte) (key_bytes.length & 0xFF);

            byte[] right_child_id_bytes = new byte[2];
            right_child_id_bytes[0] = (byte) (children[i+1] >> 8 & 0xFF);
            right_child_id_bytes[1] = (byte) (children[i+1] & 0xFF);

            this.write_data(offset_to_insert, key_len_bytes);
            offset_to_insert += 2;
            this.write_data(offset_to_insert, key_bytes);
            offset_to_insert += key_bytes.length;
            this.write_data(offset_to_insert, right_child_id_bytes);
            offset_to_insert += 2;

            i++;
        }

        byte[] next_free_offset_bytes = new byte[2];
        next_free_offset_bytes[0] = (byte) ((offset_to_insert >> 8) & 0xFF);
        next_free_offset_bytes[1] = (byte) (offset_to_insert & 0xFF);

        num_keys++;
        byte[] num_keys_bytes = new byte[2];
        num_keys_bytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        num_keys_bytes[1] = (byte) (num_keys & 0xFF);

        this.write_data(2, next_free_offset_bytes);
        this.write_data(0, num_keys_bytes);

        return;

    }

    public void insert2(T key, int right_block_id) {
        T[] keys = getKeys();
        int[] children = getChildren();

        int num_keys = getNumKeys();

        byte[] off = this.get_data(2, 2);

//        int offset_to_insert = offsetToInsert(i);
        int offset_to_insert = (off[0] & 0xFF << 8) | (off[1] & 0xFF);

        byte[] keyBytes = TtoBytes_conversion(key);
        byte[] key_length_bytes = new byte[2];
        key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);

        byte[] right_childId_bytes = new byte[2];
        right_childId_bytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        right_childId_bytes[1] = (byte) (right_block_id & 0xFF);

        this.write_data(offset_to_insert, key_length_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, keyBytes);
        offset_to_insert += keyBytes.length;
        this.write_data(offset_to_insert, right_childId_bytes);
        offset_to_insert += 2;


        byte[] next_free_offset_bytes = new byte[2];
        next_free_offset_bytes[0] = (byte) ((offset_to_insert >> 8) & 0xFF);
        next_free_offset_bytes[1] = (byte) (offset_to_insert & 0xFF);

        num_keys++;
        byte[] num_keys_bytes = new byte[2];
        num_keys_bytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        num_keys_bytes[1] = (byte) (num_keys & 0xFF);

        this.write_data(2, next_free_offset_bytes);
        this.write_data(0, num_keys_bytes);

        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        if (key == null) {
            return -1;
        }

        int num_keys = getNumKeys();
        T[] keys = getKeys();

        /* Write your code here */

        int i = 0;
        while (i < num_keys) {
            if (compare_equal(key, keys[i])) {
                return i;
            } else if (compare_greaterThan(key, keys[i])) {
                i++;
            } else {
                return -1;
            }
        }
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */

        int offset = 4;
        int i = 0;

        while (i <= numKeys) {
            byte[] childId_bytes = this.get_data(offset, 2);
            int childId = ((childId_bytes[0] & 0xFF) << 8) | (childId_bytes[1] & 0xFF);
            children[i] = childId;

            byte[] key_length_bytes = this.get_data(offset+2, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);

            offset += 4 + key_length;
            i++;
        }

        return children;

    }

}
