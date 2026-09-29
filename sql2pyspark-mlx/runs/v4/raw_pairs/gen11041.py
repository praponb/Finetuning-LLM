from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .cube("brand", "quarter_start")
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("stock_qty_per_home"),
        F.grouping("brand").alias("grouping_level"),
    )
)
