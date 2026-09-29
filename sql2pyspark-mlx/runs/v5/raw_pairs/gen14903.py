from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-24").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.substring("brand", 1, 2).alias("brand_prefix"),
    )
)

result = (
    valid
    .groupBy("supplier_id")
    .pivot("category", ["Electronics", "Grocery"])
    .agg(F.sum("price").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
