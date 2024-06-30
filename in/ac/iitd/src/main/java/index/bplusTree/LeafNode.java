package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */

        int offset = 8;
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

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */

        int offset = 8;
        int i = 0;
        while(i < numKeys) {
            byte[] blockId_bytes = this.get_data(offset, 2);
            int blockId = ((blockId_bytes[0] & 0xFF) << 8) | (blockId_bytes[1] & 0xFF);
//            System.out.println("block id getting inserted: " + blockId);
            block_ids[i] = blockId;

            byte[] key_lengthBytes = this.get_data(offset+2, 2);
            int key_length = ((key_lengthBytes[0] & 0xFF) << 8) | (key_lengthBytes[1] & 0xFF);
            offset += 4 + key_length;
            i++;
        }

        return block_ids;
    }

    private int offsetToInsert(int end) {

        int offset = 8;
        int i = 0;

        while (i < end) {
            byte[] key_lengthBytes = this.get_data(offset+2, 2);
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
    public void insert(T key, int block_id) {

        if (key == null) {
            return;
        }

        int num_keys = getNumKeys();

        /* Write your code here */

        T[] keys = getKeys();
        int[] block_ids = getBlockIds();

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

        byte[] block_id_bytes = new byte[2];
        block_id_bytes[0] = (byte) ((block_id >> 8) & 0xFF);
        block_id_bytes[1] = (byte) (block_id & 0xFF);

        int x = (block_id_bytes[0] & 0xFF << 8) | (block_id_bytes[1] & 0xFF);

        byte[] keyBytes = TtoBytes_conversion(key);
        byte[] key_length_bytes = new byte[2];
        key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);

        this.write_data(offset_to_insert, block_id_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, key_length_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, keyBytes);
        offset_to_insert += keyBytes.length;

        while (i < num_keys) {
            byte[] blockId_bytes = new byte[2];
            blockId_bytes[0] = (byte) ((block_ids[i] >> 8) & 0xFF);
            blockId_bytes[1] = (byte) (block_ids[i] & 0xFF);

            int y = (block_id_bytes[0] & 0xFF << 8) | (block_id_bytes[1] & 0xFF);

            byte[] key_bytes = TtoBytes_conversion(keys[i]);
            byte[] key_len_bytes = new byte[2];
            key_len_bytes[0] = (byte) ((key_bytes.length >> 8) & 0xFF);
            key_len_bytes[1] = (byte) (key_bytes.length & 0xFF);

            this.write_data(offset_to_insert, blockId_bytes);
            offset_to_insert += 2;
            this.write_data(offset_to_insert, key_len_bytes);
            offset_to_insert += 2;
            this.write_data(offset_to_insert, key_bytes);
            offset_to_insert += keyBytes.length;

            i++;
        }

        byte[] next_free_offset_bytes = new byte[2];
        next_free_offset_bytes[0] = (byte) ((offset_to_insert >> 8) & 0xFF);
        next_free_offset_bytes[1] = (byte) (offset_to_insert & 0xFF);

        num_keys++;
        byte[] num_keys_bytes = new byte[2];
        num_keys_bytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        num_keys_bytes[1] = (byte) (num_keys & 0xFF);

        this.write_data(6, next_free_offset_bytes);
        this.write_data(0, num_keys_bytes);

        return;

    }

    public void insert2(T key, int block_id) {
        int num_keys = getNumKeys();

        /* Write your code here */

        T[] keys = getKeys();
        int[] block_ids = getBlockIds();


//        int offset_to_insert = offsetToInsert(i);
        byte[] off = this.get_data(6, 2);

        int offset_to_insert = (off[0] & 0xFF << 8) | (off[1] & 0xFF);

        byte[] block_id_bytes = new byte[2];
        block_id_bytes[0] = (byte) ((block_id >> 8) & 0xFF);
        block_id_bytes[1] = (byte) (block_id & 0xFF);

        int x = (block_id_bytes[0] & 0xFF << 8) | (block_id_bytes[1] & 0xFF);

        byte[] keyBytes = TtoBytes_conversion(key);
        byte[] key_length_bytes = new byte[2];
        key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
        key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);

        this.write_data(offset_to_insert, block_id_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, key_length_bytes);
        offset_to_insert += 2;
        this.write_data(offset_to_insert, keyBytes);
        offset_to_insert += keyBytes.length;


        byte[] next_free_offset_bytes = new byte[2];
        next_free_offset_bytes[0] = (byte) ((offset_to_insert >> 8) & 0xFF);
        next_free_offset_bytes[1] = (byte) (offset_to_insert & 0xFF);

        num_keys++;
        byte[] num_keys_bytes = new byte[2];
        num_keys_bytes[0] = (byte) ((num_keys >> 8) & 0xFF);
        num_keys_bytes[1] = (byte) (num_keys & 0xFF);

        this.write_data(6, next_free_offset_bytes);
        this.write_data(0, num_keys_bytes);

        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        if (key == null) {
            return -1;
        }

        /* Write your code here */
        int num_keys = getNumKeys();

        int offset = 8;
        int i = 0;
        while (i < num_keys) {
            byte[] blockId_bytes = this.get_data(offset, 2);
            int blockId = (blockId_bytes[0] << 8) | (blockId_bytes[1] & 0xFF);

            byte[] key_length_bytes = this.get_data(offset+2, 2);
            int key_length = (key_length_bytes[0] << 8) | (key_length_bytes[1] & 0xFF);

            offset += 4;
            byte[] keyBytes = this.get_data(offset, key_length);
            offset += key_length;

            if (convertBytesToT(keyBytes, this.typeClass) == key) {
                return blockId;
            }
            i++;
        }

        return -1;
    }

}
