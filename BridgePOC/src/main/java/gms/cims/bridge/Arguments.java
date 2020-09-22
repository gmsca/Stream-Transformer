package gms.cims.bridge;

import java.util.ArrayList;
import java.util.Arrays;

public class Arguments {
    public static String Broker ="localhost:29092";
    public static ArrayList<String> Topics = new ArrayList(Arrays.asList("CIMSTEST.Customer.ClaimBlackList", "CIMSTEST.Financial.ClaimCostPlus"));
    public static String CCPTopic = "CIMSTEST.Financial.ClaimsCostPlus";
    public static String SchemaRegistry = "http://localhost:8081";
    public static String GroupId = "cimstest";
}
