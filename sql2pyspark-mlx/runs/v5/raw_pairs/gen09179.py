from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("order_date").asc_nulls_last())
w3 = Window.partitionBy("carrier").orderBy(F.col("carrier_length").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy("carrier_length", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy(F.col("carrier_length").desc_nulls_last())

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").isNotNull())
    .select("e.*", F.col("o.shipping_fee"), F.col("o.order_date"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "carrier", "order_date")
)

deduped_shipments = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "carrier")
)

base = (
    deduped_shipments
    .filter(F.col("carrier").isNull())
    .select(
        "*",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        F.length("carrier").alias("carrier_length"),
    )
)

valid = (
    base
    .filter(F.col("carrier").isNotNull() & F.col("carrier_length").between(28, 41))
    .select("*", F.substring("carrier", 1, 3).alias("carrier_prefix"))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .select(
        "id",
        "carrier",
        "carrier_prefix",
        F.avg("carrier_length").over(w4).alias("carrier_length_avg_6"),
        F.cume_dist().over(w5).alias("carrier_length_cume_dist"),
    )
)

result = (
    enriched
    .withColumn("carrier_length_cume_dist_tier", F.when(F.col("carrier_length_cume_dist") >= 0.77, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["carrier_length_cume_dist_tier", "carrier"], ["carrier_length_cume_dist_tier"]],
        "carrier_length_cume_dist_tier",
        "carrier",
    )
    .agg(
        F.avg(F.when(F.col("carrier") == "Kerry", F.col("carrier_length_avg_6"))).alias("kerry_avg_carrier_length_avg_6"),
        F.round(F.sum("carrier_length_cume_dist") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("carrier_length_cume_dist_per_ups"),
        F.round(F.sum("carrier_length_avg_6") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("carrier_length_avg_6_per_kerry"),
        F.round(F.sum("carrier_length_avg_6") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("carrier_length_avg_6_per_ups"),
        F.grouping("carrier_length_cume_dist_tier").alias("grouping_id"),
    )
    .orderBy(F.col("kerry_avg_carrier_length_avg_6").desc())
)
