from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("quantity", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("unit_price_whole_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last_known_unit_price_whole", "id")
w7 = Window.orderBy(F.col("cumulative_last_known_unit_price_whole").desc(), "id")
w8 = Window.orderBy("unit_price_whole_avg_6_prior_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("unit_price_whole_avg_6_prior_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.orderBy("last_known_unit_price_whole_bucket", "id")
w11 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").isNotNull())
    .withColumn("unit_price_whole", F.floor("unit_price"))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

enriched = (
    filtered
    .select(
        "id",
        "quantity",
        "unit_price_whole",
        F.last("unit_price_whole", ignorenulls=True).over(w).alias("last_known_unit_price_whole"),
        F.rank().over(w2).alias("unit_price_rank"),
        F.avg("unit_price_whole").over(w3).alias("unit_price_whole_avg_6_prior"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "quantity",
        "unit_price_whole",
        "unit_price_rank",
        F.avg("unit_price_whole_avg_6_prior").over(w4).alias("unit_price_whole_avg_6_prior_avg_5_prior"),
        F.sum("last_known_unit_price_whole").over(w5).alias("cumulative_last_known_unit_price_whole"),
        F.ntile(5).over(w6).alias("last_known_unit_price_whole_bucket"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "unit_price_whole",
        "unit_price_rank",
        "cumulative_last_known_unit_price_whole",
        "last_known_unit_price_whole_bucket",
        F.row_number().over(w7).alias("event_seq"),
        F.first("last_known_unit_price_whole_bucket", ignorenulls=True).over(w8).alias("first_known_last_known_unit_price_whole_bucket"),
        F.avg("unit_price_whole").over(w9).alias("unit_price_whole_avg_6"),
        F.lead("quantity", 1).over(w10).alias("next_quantity"),
        F.first("quantity", ignorenulls=True).over(w11).alias("first_known_quantity"),
    )
)

cleaned = (
    scored
    .filter(F.col("unit_price_whole") == 50)
    .select(
        "*",
        F.least("last_known_unit_price_whole_bucket", "unit_price_whole_avg_6").alias("least_last_known_unit_price_whole_bucket_unit_price_whole_avg_6"),
        (F.col("unit_price_rank") * F.col("first_known_last_known_unit_price_whole_bucket")).alias("unit_price_rank_times_first_known_last_known_unit_price_whole_bucket"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("last_known_unit_price_whole_bucket") >= 8, "LARGE").otherwise("SMALL").alias("last_known_unit_price_whole_bucket_tier"),
        F.when(F.col("next_quantity") >= 8, "HIGH").otherwise("LOW").alias("next_quantity_tier"),
        F.when(F.col("unit_price_whole_avg_6") >= 69, "HIGH").otherwise("LOW").alias("unit_price_whole_avg_6_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("unit_price_whole"), 2).alias("avg_unit_price_whole"),
    )
)
