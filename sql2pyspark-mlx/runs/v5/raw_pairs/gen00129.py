from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("quantity_whole", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("prev_quantity_whole", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy(F.col("quantity_whole_avg_6").desc(), "id")
w7 = Window.orderBy("quantity_whole_avg_6_bucket", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy(F.col("quantity").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") >= 300)
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("quantity_whole", F.floor("quantity"))
)

with_windows = (
    valid
    .select(
        "id",
        "quantity",
        "quantity_whole",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.lag("quantity_whole", 1).over(w2).alias("prev_quantity_whole"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.avg("quantity_whole").over(w4).alias("quantity_whole_avg_6"),
    )
)

enriched = (
    with_windows
    .filter(F.col("quantity_bucket") <= 5)
    .withColumn("last3_max_quantity_whole", F.max("quantity_whole").over(w5))
    .withColumn("last3_count_quantity_whole", F.count("quantity_whole").over(w5))
    .withColumn("last3_min_quantity_whole", F.min("quantity_whole").over(w5))
    .withColumn("quantity_whole_avg_6_bucket", F.ntile(3).over(w6))
)

cleaned = (
    enriched
    .filter(F.col("quantity_whole_avg_6_bucket").between(6, 7))
    .select(
        "id",
        "quantity",
        "quantity_whole",
        "quantity_bucket",
        "unit_price_share",
        "last3_max_quantity_whole",
        "last3_count_quantity_whole",
        "last3_min_quantity_whole",
        "quantity_whole_avg_6_bucket",
        F.coalesce("last3_max_quantity_whole", F.lit(0)).alias("last3_max_quantity_whole_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.count("last3_min_quantity_whole").over(w7).alias("last7_count_last3_min_quantity_whole"),
        F.sum("last3_min_quantity_whole").over(w7).alias("last7_sum_last3_min_quantity_whole"),
        F.avg("last3_min_quantity_whole").over(w7).alias("last7_avg_last3_min_quantity_whole"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_bucket_tier"),
        F.when(F.col("last3_max_quantity_whole_filled") >= 61, "LARGE").otherwise("SMALL").alias("last3_max_quantity_whole_filled_tier"),
        F.when(F.col("last3_max_quantity_whole") >= 86, "PREMIUM").otherwise("STANDARD").alias("last3_max_quantity_whole_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("last7_count_last3_min_quantity_whole"), 2).alias("avg_last7_count_last3_min_quantity_whole"),
        F.round(F.avg("last3_max_quantity_whole"), 2).alias("avg_last3_max_quantity_whole"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_last3_max_quantity_whole") < 1000000)
)
