from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("carrier_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status", "order_id").orderBy("carrier_length")
w5 = Window.orderBy("delivered_date", "id")
w6 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status", "carrier").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("next_cumulative_cost", "id")
w9 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w10 = Window.partitionBy("status")
w11 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w12 = Window.partitionBy("status", "order_id").orderBy("carrier_length", "id").rowsBetween(-6, Window.currentRow)
w13 = Window.partitionBy("order_id").orderBy("next_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-03-20").cast("date"))
    .withColumn("carrier_length", F.length("carrier"))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .filter(F.col("carrier_length").between(35, 44))
    .select(
        "*",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.sum("carrier_length").over(w3).alias("cumulative_carrier_length"),
        F.dense_rank().over(w_running).alias("carrier_length_dense_rank"),
    )
)

metrics = (
    scored
    .withColumn("next_cumulative_cost", F.lead("cumulative_cost", 1).over(w5))
    .withColumn("cumulative_cost_2", F.sum("cost").over(w6))
    .withColumn("cumulative_carrier_length_avg_6", F.avg("cumulative_carrier_length").over(w7))
)

scored_shipments = (
    metrics
    .select(
        "id",
        "order_id",
        "delivered_date",
        "status",
        "carrier_length",
        "cumulative_cost",
        F.lead("cost", 1).over(w8).alias("next_cost"),
        F.ntile(5).over(w9).alias("cost_bucket"),
        F.avg("cumulative_carrier_length").over(w10).alias("status_avg_cumulative_carrier_length"),
    )
)

result = (
    scored_shipments
    .select(
        "id",
        "order_id",
        "status",
        "carrier_length",
        "cumulative_cost",
        "next_cost",
        "cost_bucket",
        "status_avg_cumulative_carrier_length",
        F.row_number().over(w11).alias("event_seq"),
        F.avg("status_avg_cumulative_carrier_length").over(w12).alias("status_avg_cumulative_carrier_length_avg_6"),
        F.last("status_avg_cumulative_carrier_length", ignorenulls=True).over(w13).alias("last_known_status_avg_cumulative_carrier_length"),
    )
)
