from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("carrier", "status").orderBy(F.col("delivered_date_period").desc_nulls_last())
w_recent = Window.orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("last4_min_cost_int", "id")
w_grp = Window.partitionBy("status")

base = (
    df_shipments
    .filter(F.col("delivered_date") >= F.lit("2023-03-26").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.col("cost").cast("int").alias("cost_int"),
        F.date_trunc("MONTH", "delivered_date").alias("delivered_date_period"),
    )
)

with_windows = (
    base
    .filter(F.col("delivered_date_period") < "2023-04-14")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "cost_int",
        "delivered_date_period",
        F.min("cost_int").over(w_hist).alias("last4_min_cost_int"),
        F.count("cost_int").over(w_hist).alias("last4_count_cost_int"),
        F.sum("cost_int").over(w_hist).alias("last4_sum_cost_int"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows_shipments = (
    deduped
    .select(
        "*",
        F.sum("last4_count_cost_int").over(w_recent).alias("running_last4_count_cost_int"),
        F.lag("last4_count_cost_int", 1).over(w4).alias("prev_last4_count_cost_int"),
        F.max("last4_count_cost_int").over(w_grp).alias("status_max_last4_count_cost_int"),
    )
)

result = (
    with_windows_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "last4_min_cost_int",
        "last4_count_cost_int",
        "row_num",
        F.when(F.col("status").rlike("^(DELIVERED|LOST)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
)
