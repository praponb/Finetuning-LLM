from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-10-13"))) & F.col("brand").isNotNull() & F.col("brand").isin(["Globex", "Umbrella"]))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    base
    .groupBy("supplier_id")
    .pivot("brand", ["Stark", "Globex"])
    .agg(F.max("price_per_stock_qty").alias("value_sum"))
    .select(
        "supplier_id",
        F.col("Stark").alias("stark"),
        F.col("Globex").alias("globex"),
    )
)
