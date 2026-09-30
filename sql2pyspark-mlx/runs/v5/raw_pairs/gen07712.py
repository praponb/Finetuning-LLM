from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("cost_per_stark"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.min("cost").alias("min_cost"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
