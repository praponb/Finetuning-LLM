from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id")
w2 = Window.partitionBy("carrier")
w3 = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id")
w4 = Window.partitionBy("order_id")
w5 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("row_seq", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("carrier", "status").orderBy("row_seq", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "Flash"]))
    .withColumn("status_upper", F.upper("status"))
)

with_windows = (
    valid
    .withColumn("cost_bucket", F.ntile(10).over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w3))
)

scored = (
    with_windows
    .filter(F.col("shipped_date").between("2024-01-01", "2024-02-01"))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "status_upper",
        "cost_bucket",
        "row_seq",
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_2"),
        F.sum("cost_share").over(w5).alias("cumulative_cost_share"),
        F.avg("row_seq").over(w6).alias("row_seq_avg_3_prior"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
        F.lead("cost", 2, 0).over(w8).alias("next_cost"),
    )
)

cleaned = (
    scored
    .filter(F.col("shipped_date") > F.lit("2023-07-28").cast("date"))
    .select(
        "id",
        "delivered_date",
        "cost",
        "cost_bucket",
        "row_seq",
        "cost_share_2",
        "cumulative_cost_share",
        "row_seq_avg_3_prior",
        "row_seq_share",
        F.least("cost", "cost_bucket").alias("least_cost_cost_bucket"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "cost_bucket",
        "cost_share_2",
        "cumulative_cost_share",
        "row_seq_avg_3_prior",
    )
)
