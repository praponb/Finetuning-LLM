from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") <= 252) & F.col("supplier_id").isin([5, 14, 25]))
    .select("id", "launch_date", F.coalesce("price", F.lit(0)).alias("price_filled"))
    .orderBy(F.col("launch_date").desc(), F.col("id").desc())
)
