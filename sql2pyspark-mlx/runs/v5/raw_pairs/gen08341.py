from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("delivered_date", "id")
w4 = Window.partitionBy("carrier", "status")
w5 = Window.partitionBy("carrier").orderBy(F.col("cost_avg_2_prior").desc_nulls_last())
w_grp = Window.partitionBy("carrier", "status").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status", "carrier").orderBy("cost_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED"]))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

with_windows = (
    cleaned
    .select(
        "id",
        "carrier",
        "status",
        "status_title",
        F.count("cost").over(w).alias("order_id_count_cost"),
        F.avg("cost").over(w2).alias("cost_avg_2_prior"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status").isin(["RETURNED", "DELIVERED"]))
    .withColumn("carrier_min_order_id_count_cost", F.min("order_id_count_cost").over(w4))
    .withColumn("cost_avg_2_prior_percent_rank", F.percent_rank().over(w5))
)

with_windows_shipments = (
    enriched
    .select(
        "*",
        F.min("cost_avg_2_prior_percent_rank").over(w_grp).alias("running_min_cost_avg_2_prior_percent_rank"),
        F.count("cost_avg_2_prior_percent_rank").over(w_grp).alias("running_count_cost_avg_2_prior_percent_rank"),
        F.sum("cost_avg_2_prior_percent_rank").over(w_grp).alias("running_sum_cost_avg_2_prior_percent_rank"),
        F.avg("cost_avg_2_prior_percent_rank").over(w_grp).alias("running_avg_cost_avg_2_prior_percent_rank"),
        F.sum("order_id_count_cost").over(w_part).alias("running_order_id_count_cost"),
    )
)

result = (
    with_windows_shipments
    .select(
        "id",
        "carrier",
        "status",
        "order_id_count_cost",
        "cost_avg_2_prior",
        "carrier_min_order_id_count_cost",
        "running_count_cost_avg_2_prior_percent_rank",
        "running_sum_cost_avg_2_prior_percent_rank",
        "running_avg_cost_avg_2_prior_percent_rank",
    )
    .orderBy(F.col("carrier").desc(), F.col("id").desc())
)
