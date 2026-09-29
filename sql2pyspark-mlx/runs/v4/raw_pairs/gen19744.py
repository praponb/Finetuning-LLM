from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("category").desc_nulls_first())
)
