package Storage.LLD;

import Storage.Abstract.AbstractBlock;

// Similar to blocks of data on disk
public class Block extends AbstractBlock{
    
    public Block(byte[] data) {
        super(data);
    }

    public Block(){
        super();
    }
    
}