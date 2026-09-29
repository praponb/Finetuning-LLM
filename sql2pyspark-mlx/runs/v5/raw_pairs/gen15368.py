from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date_period").desc(), "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("carrier").orderBy(F.col("adjusted_cost").desc())

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-06-19").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "*",
        F.length("carrier").alias("carrier_length"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "carrier_filled",
        F.date_trunc("WEEK", "shipped_date").alias("shipped_date_period"),
        F.when(F.col("carrier_length") >= 67, "High").when(F.col("carrier_length") >= 16, "Medium").otherwise("Low").alias("carrier_length_tier"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "carrier_filled",
        "carrier_length_tier",
    )
)

remaining_keys = (
    deduped
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("carrier") == "FedEx")
    .select(
        "*",
        F.round((F.col("cost") * 0.9), 2).alias("adjusted_cost"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled_2"),
    )
)

metrics = (
    valid
    .filter(F.col("cost") >= 52)
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
    .withColumn("carrier_stddev_samp_adjusted_cost", F.stddev_samp("adjusted_cost").over(w3))
    .withColumn("adjusted_cost_rank", F.rank().over(w4))
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select("src.*", F.col("o.channel"), F.col("o.shipping_fee"), F.col("o.status"))
)

result = (
    joined
    .select(
        "carrier",
        "delivered_date",
        "carrier_filled",
        "adjusted_cost",
        "cost_dense_rank",
        "carrier_stddev_samp_adjusted_cost",
        "channel",
        "shipping_fee",
        "status",
    )
    .orderBy("delivered_date", F.col("carrier").desc())
)
