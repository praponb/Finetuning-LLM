from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3300, "PREMIUM").otherwise("STANDARD"))
    .rollup("revenue_tier", "store_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("units").alias("units_bit_and"),
        F.grouping("revenue_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy("distinct_ids")
)
