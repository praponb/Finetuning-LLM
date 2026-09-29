from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity_times_quantity_bucket").asc_nulls_last(), "id")
w6 = Window.orderBy("seq_no", "id")
w7 = Window.orderBy("quantity_share", "id")
w8 = Window.orderBy("quantity_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_order_items
    .filter(F.col("order_id").isin([2, 9, 31]))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w)), 4))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("quantity_bucket", F.ntile(4).over(w3))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w4))
)

valid = (
    scored
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_share",
        "seq_no",
        "quantity_bucket",
        (F.col("quantity") * F.col("quantity_bucket")).alias("quantity_times_quantity_bucket"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "unit_price",
        "quantity_share",
        "seq_no",
        "quantity_bucket",
        "quantity_times_quantity_bucket",
    )
)

scored_order_items = (
    latest
    .select(
        "id",
        "seq_no",
        "quantity_times_quantity_bucket",
        F.ntile(10).over(w6).alias("seq_no_bucket"),
        F.lead("unit_price", 1, 0).over(w7).alias("next_unit_price"),
        F.last("quantity_share", ignorenulls=True).over(w8).alias("last_known_quantity_share"),
    )
)

result = (
    scored_order_items
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.round(F.avg("last_known_quantity_share"), 2).alias("avg_last_known_quantity_share"),
        F.percentile_approx("quantity_times_quantity_bucket", 0.9).alias("quantity_times_quantity_bucket_p90"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
    )
    .orderBy(F.col("seq_no_p50").desc_nulls_last())
)
