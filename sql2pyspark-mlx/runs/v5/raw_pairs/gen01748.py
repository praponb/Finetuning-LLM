from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy("last7_max_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("last7_count_cost").asc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy("order_date", "id")
w_running = Window.orderBy("last7_count_cost", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("row_seq", "id")
w8 = Window.orderBy("last4_sum_running_order_discount", "id")

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.floor("cost").alias("cost_whole"),
        F.when(F.col("status").isin(["RETURNED", "IN_TRANSIT"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

scored = (
    cleaned
    .filter(F.col("carrier").isin(["UPS", "FedEx", "Kerry"]))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost_whole",
        "status_group",
        F.sum("cost").over(w).alias("last7_sum_cost"),
        F.avg("cost").over(w).alias("last7_avg_cost"),
        F.count("cost").over(w).alias("last7_count_cost"),
        F.max("cost").over(w).alias("last7_max_cost"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.discount"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.last7_count_cost",
        "e.last7_max_cost",
        F.col("b.discount").alias("order_discount"),
        F.col("b.shipping_fee"),
        F.col("b.order_date"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "last7_count_cost",
        "last7_max_cost",
        "order_discount",
        "shipping_fee",
        F.sum("order_discount").over(w3).alias("running_order_discount"),
        F.dense_rank().over(w4).alias("last7_count_cost_dense_rank"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

enriched_shipments = (
    metrics
    .select(
        "*",
        F.sum("running_order_discount").over(w_running).alias("last4_sum_running_order_discount"),
        F.min("running_order_discount").over(w_running).alias("last4_min_running_order_discount"),
        F.max("running_order_discount").over(w_running).alias("last4_max_running_order_discount"),
    )
)

scored_shipments = (
    enriched_shipments
    .withColumn("prev_last7_max_cost", F.lag("last7_max_cost", 1).over(w7))
    .withColumn("next_last4_sum_running_order_discount", F.lead("last4_sum_running_order_discount", 2).over(w8))
)

stats = (
    scored_shipments
    .groupBy(
        F.when(F.col("shipping_fee") >= 41, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.percentile_approx("order_discount", [0.25, 0.5, 0.75]).alias("order_discount_quartiles"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.percentile_approx("last4_min_running_order_discount", [0.25, 0.5, 0.75]).alias("last4_min_running_order_discount_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select("shipping_fee_tier", "row_seq_p50", "distinct_ids")
    .orderBy(F.col("shipping_fee_tier").desc_nulls_last(), F.col("row_seq_p50").desc())
)
