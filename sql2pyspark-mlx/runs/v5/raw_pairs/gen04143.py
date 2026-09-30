from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy(F.col("last_known_line_total").asc_nulls_last(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("line_total_p50").desc())

base = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(
        "*",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.round("unit_price", 2).alias("unit_price_rounded"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("line_total", ignorenulls=True).over(w2).alias("last_known_line_total"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    deduped
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("corr_quantity_unit_price") + F.col("row_seq")).alias("corr_quantity_unit_price_plus_row_seq"),
    )
)

result = (
    cleaned
    .groupBy(
        "product_id",
        F.when(F.col("line_total") >= 74, "HIGH").otherwise("LOW").alias("line_total_tier"),
    )
    .agg(
        F.round(F.avg("line_total"), 2).alias("avg_line_total"),
        F.percentile_approx("line_total", 0.5).alias("line_total_p50"),
        F.percentile_approx("line_total", 0.95).alias("line_total_p95"),
    )
    .filter(F.col("line_total_p95") >= 0)
    .withColumn("line_total_p50_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("line_total_p95").asc_nulls_first())
)
