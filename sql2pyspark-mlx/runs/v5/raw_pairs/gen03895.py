from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("carrier_length").asc_nulls_first())
w4 = Window.orderBy("carrier_length_percent_rank")
w5 = Window.orderBy(F.col("carrier_length_percent_rank").desc_nulls_last())

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.channel"))
)

valid = (
    with_dims
    .filter(F.col("channel").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .filter(F.year("delivered_date") >= 2024)
    .select(
        "*",
        F.max("carrier_length").over(w2).alias("running_max_carrier_length"),
        F.sum("carrier_length").over(w2).alias("running_sum_carrier_length"),
        F.avg("carrier_length").over(w2).alias("running_avg_carrier_length"),
    )
)

remaining_keys = (
    enriched
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") >= 65)
        .select("carrier")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched_shipments = (
    kept_rows
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "running_max_carrier_length",
        "running_sum_carrier_length",
        "running_avg_carrier_length",
        F.percent_rank().over(w3).alias("carrier_length_percent_rank"),
    )
)

filtered = (
    enriched_shipments
    .filter((F.col("shipped_date") > F.lit("2023-04-17").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "shipped_date",
        "running_sum_carrier_length",
        "running_avg_carrier_length",
        "carrier_length_percent_rank",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

result = (
    filtered
    .select(
        "shipped_date",
        "running_sum_carrier_length",
        "running_avg_carrier_length",
        "carrier_length_percent_rank",
        F.rank().over(w4).alias("carrier_length_percent_rank_rank"),
        F.cume_dist().over(w5).alias("carrier_length_percent_rank_cume_dist"),
    )
)
