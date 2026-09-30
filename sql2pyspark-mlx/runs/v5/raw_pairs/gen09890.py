from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.upper("product_name").like("%FFI%"))
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .groupBy("category")
    .pivot("brand", ["Acme", "Initech"])
    .agg(F.max("stock_qty").alias("amount"), F.count(F.lit(1)).alias("n"))
)
