import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/*
             Dormant Customers
       Get the customer details who have not placed any order for the month of 2014 January.
       Tables - orders and customers
       Data should be sorted in ascending order by customer_id
       Output should contain all the fields from customers

 */

class demo2{
    void output()
    {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        String path = "C:\\JavaStuff\\src\\main\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(path);

        String path_customers = "C:\\JavaStuff\\src\\main\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("com.databricks.spark.csv").option("header",true).option("inferSchema",true).load(path_customers);

        Dataset<Row> joined = orders.
                join(customers,orders.col("order_customer_id").
                        equalTo(customers.col("customer_id")),"left");

        joined = joined.filter(orders.col("order_date").like("2014-01%"))
                .orderBy(customers.col("customer_id"))
                .select(customers.col("customer_id"),customers.col("customer_firstname"),customers.col("customer_lastname")
                        ,customers.col("customer_email"),customers.col("customer_password"),customers.col("customer_street"),
                        customers.col("customer_city"),customers.col("customer_state"),customers.col("customer_zipcode"));
        joined.show();
    }

}

public class useCase2 {
    public static void main(String[] args) {
        demo2 s = new demo2();
        s.output();

    }
}
