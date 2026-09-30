from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").asc_nulls_last(), "id")
w2 = Window.partitionBy("brand").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("price", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("cost").asc_nulls_first())
w7 = Window.partitionBy("brand").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-05-29"))) & F.col("brand").isNotNull() & F.col("category").isin(["Grocery", "Electronics"]))
    .withColumn("brand_title", F.initcap(F.trim("brand")))
)

base = (
    valid
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-06-30")))
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

cleaned = (
    base
    .filter(F.col("launch_date") > F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "brand_title",
        "brand_clean",
        F.substring("product_name", 1, 2).alias("product_name_prefix"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").isNotNull()), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.last("price", ignorenulls=True).over(w3).alias("last_known_price"),
    )
)

with_windows = (
    enriched
    .withColumn("seq_no_avg_2_prior", F.avg("seq_no").over(w4))
    .withColumn("last_known_price_2", F.last("price", ignorenulls=True).over(w5))
    .withColumn("cost_percent_rank", F.percent_rank().over(w6))
    .withColumn("running_cost", F.sum("cost").over(w7))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cost_percent_rank") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("cost_percent_rank_tier"),
        "seq_no",
    )
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("seq_no")).otherwise(0)).alias("umbrella_seq_no"),
    )
    .filter(F.col("min_stock_qty") > 0)
    .orderBy(F.col("umbrella_seq_no").desc_nulls_first())
)
