from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty_filled")).otherwise(0)).alias("sports_stock_qty_filled"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("sports_stock_qty_filled") >= 0)
)
