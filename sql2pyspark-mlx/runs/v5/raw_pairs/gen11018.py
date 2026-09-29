from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("stock_qty") <= 109)
    .withColumn("product_name_prefix", F.substring("product_name", 1, 3))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

result = (
    filtered
    .select("id")
    .intersect(
        df_products
        .filter(F.col("stock_qty").between(132, 352))
        .select("id")
    )
)
