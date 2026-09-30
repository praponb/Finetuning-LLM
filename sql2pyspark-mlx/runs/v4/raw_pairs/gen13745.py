from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("cost", "id")
w_grp = Window.orderBy("is_dhl", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("cost_avg_6").asc_nulls_last())
w5 = Window.orderBy("prev_is_dhl", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id", "status")
w7 = Window.partitionBy("status").orderBy("is_dhl", "id")
w8 = Window.orderBy("is_dhl", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("status").orderBy(F.col("is_dhl").asc_nulls_last(), "id")

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-05-31").cast("date")) & F.col("carrier").isNotNull() & (F.col("shipped_date") >= "2025-09-01"))
    .select(
        "id",
        "order_id",
        "cost",
        "status",
        F.when(F.col("carrier") == "DHL", 1).otherwise(0).alias("is_dhl"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.lag("is_dhl", 1).over(w2).alias("prev_is_dhl"),
        F.sum("cost").over(w_grp).alias("running_cost"),
    )
)

enriched = (
    scored
    .withColumn("cost_avg_6_dense_rank", F.dense_rank().over(w4))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w5))
    .withColumn("order_id_sum_running_cost", F.sum("running_cost").over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("cost_avg_2", F.avg("cost").over(w8))
)

valid = (
    enriched
    .filter(F.col("order_id").isNotNull() & (F.col("cost") > 67))
    .select(
        "id",
        "status",
        "is_dhl",
        "resolved_carrier",
        "cost_avg_6",
        "prev_is_dhl",
        "cost_avg_6_dense_rank",
        "first_known_cost",
        "seq_no",
        "cost_avg_2",
        F.round((F.col("is_dhl") * 0.9), 2).alias("is_dhl_adjusted"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

base_shipments = (
    latest
    .filter(F.col("prev_is_dhl").between(29, 57))
    .select(
        "id",
        "is_dhl",
        "resolved_carrier",
        "cost_avg_6",
        "prev_is_dhl",
        "cost_avg_6_dense_rank",
        "first_known_cost",
        "seq_no",
        "cost_avg_2",
        "is_dhl_adjusted",
        "rank_in_group",
        (F.col("is_dhl_adjusted") + F.col("rank_in_group")).alias("is_dhl_adjusted_plus_rank_in_group"),
    )
)

summary = (
    base_shipments
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("is_dhl", 0.9).alias("is_dhl_p90"),
        F.round(F.avg("cost_avg_6"), 2).alias("avg_cost_avg_6"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.round(F.avg("cost_avg_2"), 2).alias("avg_cost_avg_2"),
    )
    .filter(F.col("is_dhl_p90") >= 0)
)

result = (
    summary
    .select("seq_no", "is_dhl_p90", "seq_no_quartiles", "avg_cost_avg_2")
    .orderBy(F.col("is_dhl_p90").desc_nulls_first(), "avg_cost_avg_2")
)
