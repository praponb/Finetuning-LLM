from pyspark.sql import functions as F

result = (
    df_products
    .cube(
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
        F.date_trunc("week", "launch_date").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("price"))).alias("home_total_price"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_rows"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("stock_qty_per_umbrella"),
        F.countDistinct("brand").alias("distinct_brands"),
    )
    .orderBy(F.col("stark_rows").desc_nulls_first())
)
