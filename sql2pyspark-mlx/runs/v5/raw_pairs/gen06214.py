from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("carrier", "status")
w4 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("cost_avg_2_per_cost", "id")

with_windows = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.rank().over(w2).alias("cost_rank"),
        F.count("cost").over(w3).alias("carrier_count_cost"),
        F.avg("cost").over(w4).alias("cost_avg_2"),
    )
)

base = (
    with_windows
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-07-27")))
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "cost_rank",
        "carrier_count_cost",
        "cost_avg_2",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.round((F.col("cost_avg_2") / F.col("cost")), 2).alias("cost_avg_2_per_cost"),
    )
)

scored = (
    base
    .select(
        "id",
        "shipped_date",
        "status",
        F.sum("cost_avg_2_per_cost").over(w5).alias("running_sum_cost_avg_2_per_cost"),
        F.min("cost_avg_2_per_cost").over(w5).alias("running_min_cost_avg_2_per_cost"),
        F.max("cost_avg_2_per_cost").over(w5).alias("running_max_cost_avg_2_per_cost"),
        F.count("cost_avg_2_per_cost").over(w5).alias("running_count_cost_avg_2_per_cost"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("shipped_date") < "2024-05-01")
    .select(
        "*",
        F.date_add("shipped_date", 78).alias("shipped_date_plus_78d"),
        F.round((F.col("running_count_cost_avg_2_per_cost") * 1.07), 2).alias("running_count_cost_avg_2_per_cost_scaled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "shipped_date",
        "status",
        "running_min_cost_avg_2_per_cost",
        "running_max_cost_avg_2_per_cost",
        "running_count_cost_avg_2_per_cost",
        "shipped_date_plus_78d",
        "running_count_cost_avg_2_per_cost_scaled",
    )
)
