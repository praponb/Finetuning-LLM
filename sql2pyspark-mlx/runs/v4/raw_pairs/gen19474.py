from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("launch_date").asc_nulls_first(), "id")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .withColumn("cost_tier", F.when(F.col("cost") >= 560, "HIGH").otherwise("LOW"))
    .rollup("cost_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("cost").alias("total_cost"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_cost").desc())
)
