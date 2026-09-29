from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.corr("price", "cost").alias("corr_price_cost"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("stock_qty_per_sports"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("stock_qty_per_acme"),
    )
    .orderBy("corr_price_cost")
)
