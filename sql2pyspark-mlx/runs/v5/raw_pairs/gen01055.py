from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-25").cast("date"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.brand"), F.col("p.cost"), F.col("p.category"))
)

kept_keys = (
    joined
    .select("id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") != 13)
        .select("id", "store_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

joined_store_sales = (
    kept_rows
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.cost").alias("product_cost"))
)

result = joined_store_sales.select("revenue", "sale_date_month_end", "cost", "product_cost")
