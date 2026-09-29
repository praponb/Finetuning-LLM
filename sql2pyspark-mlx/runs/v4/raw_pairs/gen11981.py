from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("adjusted_price_tier").orderBy(F.col("avg_price").desc())

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-16"))) & (F.col("quantity") >= 113))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.15), 2))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"), F.col("p.product_name"))
)

filtered = (
    with_dims
    .filter(F.col("quantity") < 215)
    .withColumn("product_name_filled", F.coalesce("product_name", F.lit("N/A")))
)

valid = (
    filtered
    .filter(F.col("last_restock") >= F.lit("2023-09-15").cast("date"))
    .withColumn("adjusted_price", F.round((F.col("price") * 0.9), 2))
    .withColumn("greatest_quantity_whole_price", F.greatest("quantity_whole", "price"))
)

grouped = (
    valid
    .groupBy(
        F.when(F.col("adjusted_price") >= 79, "PREMIUM").otherwise("STANDARD").alias("adjusted_price_tier"),
        "store_id",
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
    )
    .agg(
        F.percentile_approx("greatest_quantity_whole_price", [0.25, 0.5, 0.75]).alias("greatest_quantity_whole_price_quartiles"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .withColumn("avg_price_percent_rank", F.percent_rank().over(w))
)

result = (
    grouped
    .select(
        "adjusted_price_tier",
        "store_id",
        "year_start",
        "greatest_quantity_whole_price_quartiles",
        "avg_price",
        "price_quartiles",
        "row_count",
        "quantity_p90",
        "avg_price_percent_rank",
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
