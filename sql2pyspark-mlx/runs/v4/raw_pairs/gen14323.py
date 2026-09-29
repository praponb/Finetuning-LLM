from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_tier", F.when(F.col("units") >= 40, "LARGE").otherwise("SMALL"))
    .cube("product_id", "units_tier")
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.grouping("product_id").alias("grouping_id"),
    )
    .orderBy(F.col("num_rows").desc())
)
