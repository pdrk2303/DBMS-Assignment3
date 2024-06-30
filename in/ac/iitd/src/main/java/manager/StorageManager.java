package manager;

import index.bplusTree.BPlusTreeIndexFile;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    private Object[] convertByteArraytoObjectArray(byte[] row, String table_name) {

        if (!check_file_exists(table_name)) {
            return null;
        }

        int file_id = file_to_fileid.get(table_name);
        byte[] schemaBlock_bytes = db.get_data(file_id, 0);
        Block schemaBlock = new Block(schemaBlock_bytes);

        int num_cols = (schemaBlock_bytes[0] & 0xFF) | ((schemaBlock_bytes[1] & 0xFF) <<8);

        int fixed_cols = 0;
        int variable_cols = 0;
        int nullbitmap_offset = 0;

        int last_offset = schemaBlock.get_block_capacity()-1;

        List<Integer> typelist = new ArrayList<>();
        int j = 2;
        for (int i=0; i<num_cols; i++) {
            int col_offset = ((schemaBlock_bytes[j+1] & 0xFF) <<8) | (schemaBlock_bytes[j] & 0xFF);
            j += 2;

            byte[] col_data = schemaBlock.get_data(col_offset, last_offset-col_offset);

            int data_type = col_data[0]&0xFF;

            if (data_type == 0) {
                variable_cols++;
                typelist.add(0);
            } else if (data_type == 1) {
                fixed_cols++;
                typelist.add(1);
                nullbitmap_offset += 4;
            } else if (data_type == 2) {
                fixed_cols++;
                typelist.add(2);
                nullbitmap_offset += 1;
            } else if (data_type == 3) {
                fixed_cols++;
                typelist.add(3);
                nullbitmap_offset+=4;
            } else if (data_type == 4) {
                fixed_cols++;
                typelist.add(4);
                nullbitmap_offset+=8;
            }
            last_offset = col_offset;
        }

        int f_id = 4*variable_cols;
        int nullbitmap_index = f_id + nullbitmap_offset;

        int length = (fixed_cols + variable_cols + 7)/8;

        byte[] nullbitmap = schemaBlock.get_data(nullbitmap_index, length);

        List<Object> objectList = new ArrayList<>();


        for (int i=0; i<fixed_cols; i++) {
            int dtype = typelist.get(i);

            int byteIndex = i / 8;
            int bitIndex = i % 8;
            byte bitMask = (byte) (1 << bitIndex);

            // Read the ith bit from the byte array
            boolean bitValue = (nullbitmap[byteIndex] & bitMask) != 0;

            if (dtype == 1) {
                if (bitValue) {
                    objectList.add(null);
                } else {
                    int val = ((row[f_id+3] & 0xFF) << 24) | ((row[f_id + 2] & 0xFF) << 16) | ((row[f_id + 1] & 0xFF) << 8) | (row[f_id ] & 0xFF);
                    objectList.add(val);
                }
                f_id += 4;

            } else if (dtype == 2) {
                if (bitValue) {
                    objectList.add(null);
                } else {
                    boolean val = row[f_id] != 0;
                    objectList.add(val);
                }
                f_id += 1;

            } else if (dtype == 3) {
                if (bitValue) {
                    objectList.add(null);
                } else {
                    int val = ((row[f_id + 3] & 0xFF) << 24) | ((row[f_id + 2] & 0xFF) << 16) | ((row[f_id + 1] & 0xFF) << 8) | (row[f_id] & 0xFF);
                    objectList.add(Float.intBitsToFloat(val));
                }
                f_id += 4;

            } else if (dtype == 4) {
                if (bitValue) {
                    objectList.add(null);
                } else {
                    long val = ((row[f_id + 7] & 0xFFL) << 56) | ((row[f_id + 6] & 0xFFL) << 48) | ((row[f_id + 5] & 0xFFL) << 40) | ((row[f_id + 4] & 0xFFL) << 32) |
                            ((row[f_id + 3] & 0xFFL) << 24) | ((row[f_id + 2] & 0xFFL) << 16) | ((row[f_id + 1] & 0xFFL) << 8) | (row[f_id] & 0xFFL);
                    objectList.add(Double.longBitsToDouble(val));
                }
                f_id += 8;

            }
        }

        f_id = 0;
        Block rowBlock = new Block(row);
        for (int i=0; i<variable_cols; i++) {
            int byteIndex = (i+fixed_cols) / 8;
            int bitIndex = (i+fixed_cols) % 8;
            byte bitMask = (byte) (1 << bitIndex);
            boolean bitValue = (nullbitmap[byteIndex] & bitMask) != 0;

            if (bitValue) {
                objectList.add(null);
                f_id += 4;

            } else {
                int off_i = (row[f_id] & 0xFF) | ((row[f_id+1] & 0xFF )<< 8);
                f_id += 2;
                int len_i = (row[f_id] & 0xFF) | ((row[f_id+1] & 0xFF) << 8);
                f_id += 2;

                byte[] val_str = rowBlock.get_data(off_i, len_i);
                objectList.add(new String(val_str));
            }

        }

        return objectList.toArray();
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise

        if (!check_file_exists(table_name) || block_id < 0) {
            return null;
        }

        List<Object[]> records_list = new ArrayList<>();
        byte[] data_block_bytes = get_data_block(table_name, block_id);
        Block data_block = new Block(data_block_bytes);
        int num_rows = (data_block_bytes[1] & 0xFF) | ((data_block_bytes[0] & 0xFF) << 8);

        int offset = 2;
        int last_offset = data_block.get_block_capacity()-1;

        for (int i=0; i<num_rows; i++) {
            int row_offset = (data_block_bytes[offset+1] & 0xFF) | ((data_block_bytes[offset] & 0xFF) << 8);
            offset += 2;

            byte[] record = data_block.get_data(row_offset, last_offset-row_offset);
            Object[] record_object = convertByteArraytoObjectArray(record, table_name);
            records_list.add(record_object);

            last_offset = row_offset;
        }
        return records_list;
    }

    private int get_block_count(int file_id) {
        int count = 0;
        while (db.get_data(file_id, count) != null) {
            count++;
        }
        return count;
    }

    private List<RelDataType> convertInttoTypeList(List<Integer> type_list) {
        List<RelDataType> relTypeList = new ArrayList<>();
        RelDataTypeFactory type_factory = new JavaTypeFactoryImpl();

        for (int i=0; i<type_list.size(); i++) {
            int number = type_list.get(i);
            if (number == 0) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.VARCHAR));
            } else if (number == 1) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.INTEGER));
            } else if (number == 2) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.BOOLEAN));
            } else if (number == 3) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.FLOAT));
            } else if (number == 4) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.DOUBLE));
            } else {
                throw new RuntimeException("Unsupported data type");
            }
        }

        return relTypeList;

    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */

        if (!check_file_exists(table_name)) {
            return false;
        }
        if (check_index_exists(table_name, column_name)) {
            return false;
        }

        int file_id = file_to_fileid.get(table_name);
        byte[] schemaBlock_bytes = db.get_data(file_id, 0);
        Block schemaBlock = new Block(schemaBlock_bytes);

        int num_cols = (schemaBlock_bytes[0] & 0xFF) | ((schemaBlock_bytes[1] & 0xFF) <<8);

        int fixed_cols = 0;
        int variable_cols = 0;
        int match_idx = -1;
        int match_dtype = -1;
        int match_offset = 0;

        int last_offset = schemaBlock.get_block_capacity()-1;

        List<Integer> typelist = new ArrayList<>();
        List<String> col_names = new ArrayList<>();

        int off = 2;
        for (int i=0; i<num_cols; i++) {
            int col_offset = ((schemaBlock_bytes[off+1] & 0xFF) <<8) | (schemaBlock_bytes[off] & 0xFF);
            System.out.println("col_offset: " + col_offset);
            off += 2;

            byte[] col_data = schemaBlock.get_data(col_offset, last_offset-col_offset);

            int dtype = col_data[0]&0xFF;
            int len_col_name = col_data[1]&0xFF;
            col_offset += 2;
            byte[] col_name_bytes = schemaBlock.get_data(col_offset, len_col_name);
            col_names.add(new String(col_name_bytes));

//            if (dtype == 0 || dtype == 1 || dtype == 2 || dtype == 3 || dtype == 4) {
                typelist.add(dtype);
//            }
            last_offset = col_offset;
        }

        int f = 0;

        for (int i=0; i<col_names.size(); i++) {
            String col_name = col_names.get(i);
            if (column_name.equals(col_name)) {
                match_idx = i;
                f = 1;
            }
            int dtype = typelist.get(i);
            if (dtype == 0) {
                variable_cols++;
                if (column_name.equals(col_name)) {
                    match_dtype = 0;
                }

            } else if (dtype == 1) {
                fixed_cols++;
                if (f == 0) {
                    match_offset += 4;
                }
                if (column_name.equals(col_name)) {
                    match_dtype = 1;
                }

            } else if (dtype == 2) {
                fixed_cols++;
                if (f == 0) {
                    match_offset += 1;
                }
                if (column_name.equals(col_name)) {
                    match_dtype = 2;
                }

            } else if (dtype == 3) {
                fixed_cols++;
                if (f == 0) {
                    match_offset += 4;
                }
                if (column_name.equals(col_name)) {
                    match_dtype = 3;
                }

            } else if (dtype == 4) {
                fixed_cols++;
                if (f == 0) {
                    match_offset += 8;
                }
                if (column_name.equals(col_name)) {
                    match_dtype = 4;
                }
            }
        }

        System.out.println("num cols: " + num_cols);
        int total_blocks = get_block_count(file_id);
        List<RelDataType> rel_typelist = convertInttoTypeList(typelist);
        if (match_dtype == 0) {
            BPlusTreeIndexFile<String> indexFile = new BPlusTreeIndexFile<>(order, String.class);
            int id = 1;

            while (id < total_blocks) {
                List<Object[]> records_list = get_records_from_block(table_name, id);

                for (Object[] record: records_list) {
                    byte[] rbytes = convertToByteArray(record, rel_typelist);
                    Block rblock = new Block(rbytes);

                    match_idx = match_idx - fixed_cols;
                    int offset = 4*match_idx;
                    byte[] o_bytes = rblock.get_data(offset, 2);
                    offset += 2;
                    int o_i = (o_bytes[0] & 0xFF) | ((o_bytes[1] & 0xFF) << 8);
                    byte[] len_bytes = rblock.get_data(offset, 2);
                    int len_i = (len_bytes[0] & 0xFF) | ((len_bytes[1] & 0xFF) << 8);
                    byte[] col_val = rblock.get_data(o_i, len_i);
                    indexFile.insert(new String(col_val), id);

                }
                id++;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int cid = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, cid);

        } else if (match_dtype == 1) {
            BPlusTreeIndexFile<Integer> indexFile = new BPlusTreeIndexFile<>(order, Integer.class);
            int id = 1;

            while (id < total_blocks) {
                List<Object[]> records_list = get_records_from_block(table_name, id);

                for (Object[] record: records_list) {
                    byte[] rbytes = convertToByteArray(record, rel_typelist);
                    Block rblock = new Block(rbytes);

//                    match_idx = match_idx - fixed_cols;
                    int offset = 4*variable_cols + match_offset;
                    byte[] colb = rblock.get_data(offset, 4);
                    int val = (colb[0] & 0xFF) | ((colb[1] & 0xFF) << 8) | ((colb[2] & 0xFF) << 16) | ((colb[3] & 0xFF) << 24);

                    indexFile.insert(val, id);
                }
                id++;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int cid = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, cid);

        } else if (match_dtype == 2) {
            BPlusTreeIndexFile<Boolean> indexFile = new BPlusTreeIndexFile<>(order, Boolean.class);
            int id = 1;

            while (id < total_blocks) {
                List<Object[]> records_list = get_records_from_block(table_name, id);

                for (Object[] record: records_list) {
                    byte[] rbytes = convertToByteArray(record, rel_typelist);
                    Block rblock = new Block(rbytes);

//                    match_idx = match_idx - fixed_cols;
                    int offset = 4*variable_cols + match_offset;
                    byte[] colb = rblock.get_data(offset, 1);
                    boolean val = colb[0] != 0;
                    indexFile.insert(val, id);
                }
                id++;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int cid = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, cid);

        } else if (match_dtype == 3) {
            BPlusTreeIndexFile<Float> indexFile = new BPlusTreeIndexFile<>(order, Float.class);
            int id = 1;

            while (id < total_blocks) {
                List<Object[]> records_list = get_records_from_block(table_name, id);

                for (Object[] record: records_list) {
                    byte[] rbytes = convertToByteArray(record, rel_typelist);
                    Block rblock = new Block(rbytes);

//                    match_idx = match_idx - fixed_cols;
                    int offset = 4*variable_cols + match_offset;
                    byte[] colb = rblock.get_data(offset, 4);
                    int val = (colb[0] & 0xFF) | ((colb[1] & 0xFF) << 8) | ((colb[2] & 0xFF) << 16) | ((colb[3] & 0xFF) << 24);

                    indexFile.insert(Float.intBitsToFloat(val), id);
                }
                id++;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int cid = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, cid);

        } else if (match_dtype == 4) {
            BPlusTreeIndexFile<Double> indexFile = new BPlusTreeIndexFile<>(order, Double.class);
            int id = 1;

            while (id < total_blocks) {
                List<Object[]> records_list = get_records_from_block(table_name, id);

                for (Object[] record: records_list) {
                    byte[] rbytes = convertToByteArray(record, rel_typelist);
                    Block rblock = new Block(rbytes);

//                    match_idx = match_idx - fixed_cols;
                    int offset = 4*variable_cols + match_offset;
                    byte[] colb = rblock.get_data(offset, 8);
                    long val = (colb[0] & 0xFF) | ((colb[1] & 0xFF) << 8) | ((colb[2] & 0xFF) << 16) | ((colb[3] & 0xFF) << 24) |
                            ((colb[4] & 0xFF) << 32) | ((colb[5] & 0xFF) << 40) | ((colb[6] & 0xFF) << 48) | ((colb[7] & 0xFF) << 56);

                    indexFile.insert(Double.longBitsToDouble(val), id);
                }
                id++;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int cid = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, cid);
        }

        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        byte[] val = (byte[]) value.getValue2();
        return db.search_index(file_id, val);

    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        byte[] val = (byte[]) value.getValue2();
        return db.delete_from_index(file_id, val);
//        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}
