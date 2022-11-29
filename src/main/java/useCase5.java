import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

class demo5{
    void output()
    {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();


        String path_departments = "C:\\JavaStuff\\src\\main\\retail_db\\departments\\part-00000";
        Dataset<Row> departments = spark.read().format("com.databricks.spark.csv").
                option("header", true).option("infer schema", true).load(path_departments);

        String path_products = "C:\\JavaStuff\\src\\main\\retail_db\\products\\part-00000";
        Dataset<Row> products = spark.read().format("com.databricks.spark.csv").
                option("header",true).option("infer schema",true).load(path_products);

        String path_categories = "C:\\JavaStuff\\src\\main\\retail_db\\categories\\part-00000";
        Dataset<Row> categories = spark.read().format("com.databricks.spark.csv").
                option("header",true).option("infer schema",true).load(path_categories);

        Dataset<Row> joinDF = departments
                .join(categories,departments.col("department_id").equalTo(categories.col("category_department_id")))
                .join(products,categories.col("category_id").equalTo(products.col("product_category_id")));

        joinDF = joinDF
                .groupBy(joinDF.col("department_id"),joinDF.col("department_name"))
                .agg(count(joinDF.col("product_id")).alias("product_count"))
                .orderBy(joinDF.col("department_id"));

        joinDF.write().format("com.databricks.spark.csv").save("C:/JavaStuff/src/output/file2.csv");
    }

}

public class useCase5 {
    public static void main(String[] args) {
        demo5 s = new demo5();
        s.output();

    }
}