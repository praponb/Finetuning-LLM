from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy("quantity_bucket", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

with_windows = (
    base
    .withColumn("quantity_bucket", F.ntile(3).over(w))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w2))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.stock_qty"))
)

cleaned = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.least("prev_quantity", "quantity").alias("least_prev_quantity_quantity"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_level",
        "prev_quantity",
        "row_num",
        F.lead("quantity").over(w2).alias("next_quantity"),
        F.lead("prev_quantity").over(w2).alias("next_prev_quantity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy(
        "order_id",
        F.when(F.col("next_quantity") >= 8, "HIGH").otherwise("LOW").alias("next_quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("prev_quantity", [0.25, 0.5, 0.75]).alias("prev_quantity_quartiles"),
        F.sum("quantity").alias("total_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
    )
)
