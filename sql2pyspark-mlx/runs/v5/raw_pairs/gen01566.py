from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("brand", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category")
w3 = Window.orderBy("running_max_tags_count", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("tags_count").desc(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-08-13"))) & (F.length("product_name") < 13))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.size("tags").alias("tags_count"),
    )
)

scored = (
    valid
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "tags_count",
        F.max("tags_count").over(w_part).alias("running_max_tags_count"),
        F.count("tags_count").over(w_part).alias("running_count_tags_count"),
        F.min("tags_count").over(w_part).alias("running_min_tags_count"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        F.avg("running_count_tags_count").over(w3).alias("running_count_tags_count_avg_3"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-04-16")))
    .select("*", F.date_sub("launch_date", 64).alias("launch_date_minus_64d"))
)

matched_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("brand") == "Umbrella")
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.date_trunc("year", "launch_date_minus_64d").alias("year_start"),
        F.when(F.col("stock_qty") >= 363, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
    )
)

result = (
    totals
    .select("row_seq_tier", "year_start", "stock_qty_tier", "row_seq_p95")
    .orderBy(F.col("row_seq_tier").desc_nulls_last(), "row_seq_p95")
)
