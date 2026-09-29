from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_last())
w3 = Window.partitionBy("order_id", "carrier").orderBy("shipped_date", "id")
w4 = Window.partitionBy("carrier").orderBy("cost_rank", "id")
w5 = Window.partitionBy("status", "order_id")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("carrier", "status")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select("*", F.concat("carrier", F.lit(", "), "status").alias("carrier_status"))
)

enriched = (
    valid
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_shipments = (
    ranked
    .withColumn("next_first_known_cost", F.lead("first_known_cost", 2).over(w4))
    .withColumn("cost_rank_share", F.round((F.col("cost_rank") / F.sum("cost_rank").over(w5)), 4))
    .withColumn("cost_percent_rank", F.percent_rank().over(w6))
)

joined = (
    enriched_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.shipping_fee"), F.col("d.amount"))
)

matched_keys = (
    joined
    .select("id", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.col("cost").between(45, 71))
        .select("id", "carrier")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("status_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w7))
    .withColumn("carrier_stddev_samp_first_known_cost", F.stddev_samp("first_known_cost").over(w8))
)

result = (
    scored
    .select(
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_status",
        "first_known_cost",
        "cost_rank_share",
        "cost_percent_rank",
        "amount",
    )
)
