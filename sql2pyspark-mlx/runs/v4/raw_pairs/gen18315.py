from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id")
w_running = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("carrier")
w_part = Window.partitionBy("carrier").orderBy(F.col("carrier_length").desc_nulls_last())
w7 = Window.partitionBy("carrier").orderBy("carrier_length_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("carrier").orderBy("least_next_cost_carrier_length", "id")
w9 = Window.partitionBy("carrier").orderBy("running_cost", "id").rowsBetween(-2, -1)

with_windows = (
    df_shipments
    .withColumn("next_cost", F.lead("cost", 2).over(w))
    .withColumn("running_cost", F.sum("cost").over(w_running))
    .withColumn("cost_avg_6", F.avg("cost").over(w_hist))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
)

valid = (
    with_windows
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-11-22"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "next_cost",
        "running_cost",
        "cost_share",
        F.length("carrier").alias("carrier_length"),
    )
)

base = (
    valid
    .filter((F.col("shipped_date") >= F.lit("2023-08-05").cast("date")) & F.col("carrier").isNotNull() & (F.col("carrier_length") <= 75))
    .select(
        "*",
        F.least("next_cost", "carrier_length").alias("least_next_cost_carrier_length"),
    )
)

enriched = (
    base
    .withColumn("cost_share_2", F.round((F.col("cost") / F.sum("cost").over(w5)), 4))
    .withColumn("carrier_length_cume_dist", F.cume_dist().over(w_part))
)

scored = (
    enriched
    .withColumn("last_known_least_next_cost_carrier_length", F.last("least_next_cost_carrier_length", ignorenulls=True).over(w7))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("cost_share_avg_2_prior", F.avg("cost_share").over(w9))
)

result = (
    scored
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "cost_share",
        "carrier_length",
        "least_next_cost_carrier_length",
        "cost_share_2",
    )
    .orderBy(F.col("least_next_cost_carrier_length").desc())
)
