
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/*
    Revenue Per Category

        Get the revenue generated for each category for the month of 2014 January
            Tables - orders, order_items, products and categories
            Data should be sorted in ascending order by category_id.
            Output should contain all the fields from category along with the revenue as category_revenue.
            Consider only COMPLETE and CLOSED orders
 */
class demo4{
    void output()
    {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        String path = "C:\\JavaStuff\\src\\main\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("com.databricks.spark.csv").option("header", true).option("infer schema", true).load(path);

        String path_products = "C:\\JavaStuff\\src\\main\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("com.databricks.spark.csv").option("header",true).option("infer schema",true).load(path_products);

        String path_order_items = "C:\\JavaStuff\\src\\main\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items = spark.read().format("com.databricks.spark.csv").option("header",true).option("infer schema",true).load(path_order_items);

        String path_categories = "C:\\JavaStuff\\src\\main\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("com.databricks.spark.csv").option("header",true).option("infer schema",true).load(path_categories);

        Dataset<Row> joined = orders.
                join(products,orders.col("order_customer_id").equalTo(products.col("product_id")))
                .join(order_items,orders.col("order_id").equalTo(order_items.col("order_item_order_id")))
                .join(categories,products.col("product_category_id").equalTo(categories.col("category_id")));

        joined = joined.filter(joined.col("order_date").like("2014-01%"))
                .filter( joined.col("order_status").isin("COMPLETE","CLOSED"));

        joined = joined
                .groupBy(joined.col("category_id"),joined.col("category_department_id"),joined.col("category_name"))
                .agg(round(sum(joined.col("order_item_subtotal")),2).alias("category_revenue"))
                .orderBy(joined.col("category_id"));

        joined.write().format("com.databricks.spark.csv").save("/JavaStuff/src/output/file1.csv");
    }

}
public class useCase4 {
    public static void main(String[] args) {
        demo4 s = new demo4();
        s.output();

    }
}

