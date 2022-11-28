import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.count;

/*

    Customer order count

    Get order count per customer for the month of 2014 January.
    Tables - orders and customers
    Data should be sorted in descending order by count and ascending order by customer id.
    Output should contain customer_id, customer_first_name, customer_last_name and customer_order_count.


 */

class demo1{

    void result()
    {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        String path = "C:\\JavaStuff\\src\\main\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("com.databricks.spark.csv").option("header", true).
                option("inferSchema", true).load(path);
        String path_customers = "C:\\JavaStuff\\src\\main\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = spark.read().format("com.databricks.spark.csv").
                option("header",true).option("inferSchema",true).load(path_customers);

        Dataset<Row> joinDF = orders.
                join(customers,orders.col("order_customer_id").
                        equalTo(customers.col("customer_id")),"inner");
        joinDF = joinDF.filter(orders.col("order_date").like("2014-01%")).
                groupBy(customers.col("customer_id"),customers.col("customer_firstname"),customers.col("customer_lastname"))
                .agg(count(customers.col("customer_id")).alias("customer_order_count"));

        joinDF = joinDF.orderBy(joinDF.col("customer_order_count").desc(),customers.col("customer_id"));
        joinDF.show();
    }
}

public class useCase1 {
    public static void main(String[] args) {
        demo1 s = new demo1();
        s.result();

    }
}
