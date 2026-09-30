from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w4 = Window.partitionBy("status").orderBy("cost_filled", "id")
w5 = Window.partitionBy("order_id").orderBy("delivered_date_period", "id")
w6 = Window.partitionBy("order_id").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-03-31", "2025-08-01"))
    .withColumn("delivered_date_period", F.date_trunc("MONTH", "delivered_date"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .filter(F.col("delivered_date_period") >= "2025-10-09")
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w_running))
    .withColumn("prev_cost", F.lag("cost", 1).over(w4))
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w5) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_shipments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date_period",
        "carrier_filled",
        "next_cost",
        "prev_cost",
        F.sum("is_new_burst").over(w6).alias("session_no"),
    )
)

result = (
    sessions_shipments
    .select("id", "carrier", "delivered_date_period", "prev_cost")
    .orderBy(F.col("id").desc_nulls_last())
)
