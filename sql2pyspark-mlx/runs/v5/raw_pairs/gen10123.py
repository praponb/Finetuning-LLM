from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-04-28").cast("date")) & (F.year("launch_date") >= 2025))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

result = (
    valid
    .groupBy("brand", "supplier_id")
    .pivot("category", ["Books", "Electronics"])
    .agg(F.max("price_plus_stock_qty").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "brand",
        "supplier_id",
        F.col("Books_total").alias("books_total"),
        F.col("Books_n").alias("books_n"),
        F.col("Electronics_total").alias("electronics_total"),
        F.col("Electronics_n").alias("electronics_n"),
    )
)
