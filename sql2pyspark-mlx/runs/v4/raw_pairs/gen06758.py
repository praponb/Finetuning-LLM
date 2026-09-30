from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-01-26")))
    .withColumn("quantity_rounded", F.round("quantity", 0))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("quarter", "last_restock"))
    .withColumn("quantity_rounded_tier", F.when(F.col("quantity_rounded") >= 63, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "quantity_rounded_tier", "store_id"], ["quarter_start", "quantity_rounded_tier"], ["quarter_start"], []],
        "quarter_start",
        "quantity_rounded_tier",
        "store_id",
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity_rounded"), 2).alias("avg_quantity_rounded"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("store_id")
)
