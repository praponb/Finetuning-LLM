from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("carrier", "order_id").orderBy("delivered_date_plus_12d", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("status")
w4 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("cost_share", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("status_sum_cost_share", "id")

base = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 4, 6]) & (F.col("shipped_date") > F.to_date(F.lit("2023-03-20"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.date_add("delivered_date", 12).alias("delivered_date_plus_12d"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

with_windows = (
    base
    .filter(F.col("cost") < 92)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.avg("cost").over(w_ordered).alias("cost_avg_3"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "cost",
        F.sum("cost_share").over(w_hist).alias("status_sum_cost_share"),
        F.first("cost_avg_3", ignorenulls=True).over(w4).alias("first_known_cost_avg_3"),
        F.avg("cost").over(w5).alias("cost_avg_6"),
        F.stddev_samp("cost").over(w_hist).alias("status_stddev_samp_cost"),
    )
)

with_windows_shipments = (
    metrics
    .select(
        "id",
        "cost",
        "status_sum_cost_share",
        "first_known_cost_avg_3",
        F.lead("cost_avg_6").over(w6).alias("next_cost_avg_6"),
        F.lag("cost_avg_6").over(w6).alias("prev_cost_avg_6"),
        F.lag("status_sum_cost_share").over(w6).alias("prev_status_sum_cost_share"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    with_windows_shipments
    .select(
        "id",
        "status_sum_cost_share",
        "first_known_cost_avg_3",
        "next_cost_avg_6",
        "prev_cost_avg_6",
        "prev_status_sum_cost_share",
        "row_seq",
    )
    .orderBy("id")
    .limit(20)
)
