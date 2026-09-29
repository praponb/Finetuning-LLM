from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.orderBy("shipped_date_plus_months_month_end", "id")
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc())

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-09").cast("date")) & F.col("shipped_date").between("2024-06-01", "2025-09-23"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
    )
)

valid_shipments = (
    valid
    .filter((F.col("shipped_date") > F.lit("2023-05-12").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("shipped_date_plus_months_month_end", F.last_day("shipped_date_plus_months"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("DIRECT")))
)

enriched = (
    valid_shipments
    .filter(F.col("shipped_date_plus_months_month_end") < "2025-08-01")
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "shipped_date_plus_months",
        F.stddev_samp("cost").over(w).alias("carrier_stddev_samp_cost"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.lag("cost", 1).over(w3).alias("prev_cost"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched_shipments = (
    latest
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.amount"), F.col("b.channel"), F.col("b.discount"))
)

by_type = (
    enriched_shipments
    .groupBy("order_id")
    .pivot("carrier", ["DHL", "UPS"])
    .agg(F.avg("cost"))
)

result = by_type.select("order_id")
