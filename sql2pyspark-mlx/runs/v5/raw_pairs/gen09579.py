from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-08-18").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

result = (
    base
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella"])
    .agg(F.avg("price").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
