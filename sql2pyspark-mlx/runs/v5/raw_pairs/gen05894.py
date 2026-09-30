from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("cost").desc(), "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-29").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.round("stock_qty", 1).alias("stock_qty_rounded"),
    )
)

filtered_products = (
    filtered
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").isNotNull())
    .select(
        "*",
        (F.col("cost") / F.nullif("stock_qty_rounded", F.lit(0))).alias("cost_per_stock_qty_rounded"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

deduped = (
    filtered_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

stats = (
    ranked
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count_if(F.col("category") == "Books").alias("books_events"),
        F.round(F.sum("cost_per_stock_qty_rounded") / F.nullif(F.count(F.when(F.col("category") == "Fashion", 1)), F.lit(0)), 2).alias("cost_per_stock_qty_rounded_per_fashion"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
    )
    .filter(F.col("avg_cost") < 1000000)
)

result = (
    stats
    .select("supplier_id", "stock_qty_bit_or")
    .orderBy(F.col("stock_qty_bit_or").asc_nulls_last())
)
