from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("cost", "id")
w3 = Window.partitionBy("status", "order_id")
w4 = Window.partitionBy("order_id", "status")
w5 = Window.partitionBy("status").orderBy(F.col("next_cost").asc_nulls_last())
w_recent = Window.partitionBy("status").orderBy("delivered_date", "id")
w7 = Window.partitionBy("order_id", "status").orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("status_count_cost").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("status_count_cost").asc_nulls_first())

valid = (
    df_shipments
    .filter(F.col("delivered_date") > F.to_date(F.lit("2024-03-06")))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").isin(["RETURNED", "IN_TRANSIT"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.lead("cost", 1).over(w2).alias("next_cost"),
        F.row_number().over(w2).alias("event_seq"),
        F.count("cost").over(w3).alias("status_count_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.dense_rank().over(w5).alias("next_cost_dense_rank"),
        F.lag("cost_share", 2).over(w_recent).alias("prev_cost_share"),
        F.sum("cost").over(w7).alias("cumulative_cost"),
        F.ntile(5).over(w8).alias("status_count_cost_bucket"),
        F.rank().over(w9).alias("status_count_cost_rank"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.status").alias("order_status"))
)

enriched_shipments = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.discount"),
        F.col("b.status").alias("order_status_2"),
        F.col("b.amount"),
    )
)

result = (
    enriched_shipments
    .withColumn("quarter_start", F.date_trunc("QUARTER", "delivered_date"))
    .groupingSets(
        [["quarter_start", "status"], ["quarter_start"]],
        "quarter_start",
        "status",
    )
    .agg(
        F.percentile_approx("cumulative_cost", 0.9).alias("cumulative_cost_p90"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.avg("cumulative_cost").alias("avg_cumulative_cost"),
        F.sum(F.when(F.col("order_status_2") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
        F.percentile_approx("next_cost_dense_rank", [0.25, 0.5, 0.75]).alias("next_cost_dense_rank_quartiles"),
    )
    .orderBy(F.col("avg_cumulative_cost").asc_nulls_first())
)
