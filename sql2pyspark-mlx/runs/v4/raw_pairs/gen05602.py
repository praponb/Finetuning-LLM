from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("*", F.round((F.col("cost") * 1.07), 2).alias("cost_adjusted"))
)

valid = (
    base
    .filter((F.col("delivered_date") >= F.lit("2023-06-11").cast("date")) & F.col("order_id").isNotNull() & F.col("carrier").isin(["DHL", "Flash"]))
    .select("*", F.date_trunc("week", "delivered_date").alias("delivered_date_period"))
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cost_adjusted") >= 78, "PREMIUM").otherwise("STANDARD").alias("cost_adjusted_tier"),
        "seq_no",
        F.when(F.col("cost_avg_2_prior") >= 79, "LARGE").otherwise("SMALL").alias("cost_avg_2_prior_tier"),
    )
    .agg(
        F.round(F.avg("cost_adjusted"), 2).alias("avg_cost_adjusted"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
    )
    .orderBy("cost_avg_2_prior_tier")
)
