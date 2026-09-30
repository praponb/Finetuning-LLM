from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price_times_stock_qty").desc_nulls_first(), "id")
w2 = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")
w_ordered = Window.partitionBy("supplier_id").orderBy(F.col("price_times_stock_qty").desc())
w4 = Window.partitionBy("category").orderBy("price_times_stock_qty", "id")
w_recent = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-05-25"))) & F.col("brand").isNotNull() & F.array_contains("tags", "clearance"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.least("price", "stock_qty").alias("least_price_stock_qty"),
        (F.col("price") * F.col("stock_qty")).alias("price_times_stock_qty"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w_ordered).alias("price_times_stock_qty_percent_rank"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w_recent).alias("event_seq_2"),
    )
)

totals = (
    enriched
    .groupBy(
        "supplier_id",
        F.when(F.col("price_times_stock_qty") >= 71, "HIGH").otherwise("LOW").alias("price_times_stock_qty_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("event_seq_per_grocery"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .filter(F.col("num_categories") > 1)
)

result = (
    totals
    .select(
        "supplier_id",
        "price_times_stock_qty_tier",
        "week_start",
        "fashion_count",
        "event_seq_per_grocery",
        "num_categories",
    )
    .orderBy("supplier_id", "week_start")
)
