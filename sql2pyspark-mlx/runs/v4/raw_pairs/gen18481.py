from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        "category",
        "brand",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", F.col("price")).otherwise(0)).alias("fashion_price"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .orderBy(F.col("fashion_price").asc_nulls_last())
)
