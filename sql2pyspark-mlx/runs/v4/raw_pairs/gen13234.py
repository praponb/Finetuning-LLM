from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("days_delivered_date_to_shipped_date").desc_nulls_last(), "id")

base = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-09-15").cast("date"))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

with_windows = (
    base
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("order_id")
    .pivot("status", ["LOST", "IN_TRANSIT"])
    .agg(F.max("cost_avg_2_prior"))
)
