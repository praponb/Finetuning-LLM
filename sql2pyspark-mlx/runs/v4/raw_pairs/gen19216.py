from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-09").cast("date"))
    .withColumn("revenue_scaled", F.round((F.col("revenue") * 1.07), 2))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("revenue") >= 4400, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
        "store_id",
    )
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("units").alias("max_units"),
        F.avg("revenue").alias("avg_revenue_2"),
        F.bit_and("units").alias("units_bit_and"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .filter(F.col("avg_revenue") < 0)
    .orderBy(F.col("avg_revenue_2").desc_nulls_last())
)
