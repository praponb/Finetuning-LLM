from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"), "brand")
    .agg(
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.stddev_samp("price").alias("price_stddev"),
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
