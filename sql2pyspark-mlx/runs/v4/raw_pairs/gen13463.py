from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("cost") > 440)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    base
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella", "Globex"])
    .agg(F.max("price").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
