import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;
public class DeserializeTest {

    @Test
    public void test_get_records_from_block() {
        try {
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            List<Object []> result = calciteConnection.get_records_from_block("actor", 2);
            // Uncomment this to test the function after implementing it

//            assert(result.size() == 78);
//            for(int i = 0; i < result.size(); i++){
//                assert(result.get(i).length == 4);
//                assert(result.get(i)[0] instanceof Integer);
//                assert((Integer) result.get(i)[0] == i + 78);
//            }
            calciteConnection.close();
            
        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test passed :)");
    }
}