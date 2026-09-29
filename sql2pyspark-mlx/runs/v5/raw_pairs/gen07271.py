from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("carrier")
    .subtract(
        df_shipments
        .filter(F.month("delivered_date").isin([1, 3, 5]))
        .select("carrier")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") < "2024-05-01"))
    .select(
        "carrier",
        "delivered_date",
        "cost",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

valid_shipments = (
    valid
    .filter(F.year("delivered_date") == 2023)
    .select(
        "carrier",
        "status_prefix",
        "carrier_filled",
        F.dayofweek("delivered_date").alias("delivered_date_dayofweek"),
        F.coalesce("carrier", F.lit("unassigned")).alias("carrier_filled_2"),
    )
)

filtered = (
    valid_shipments
    .filter((F.col("carrier").eqNullSafe(F.lit("Kerry"))) & (F.col("delivered_date_dayofweek") < 43))
    .select("*", F.substring("carrier", 1, 3).alias("carrier_prefix"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("delivered_date_dayofweek") >= 75, "HIGH").otherwise("LOW").alias("delivered_date_dayofweek_tier"),
    )
    .agg(
        F.round(F.avg("delivered_date_dayofweek"), 2).alias("avg_delivered_date_dayofweek"),
        F.count("*").alias("row_count"),
        F.percentile_approx("delivered_date_dayofweek", 0.5).alias("delivered_date_dayofweek_p50"),
    )
    .filter(F.col("avg_delivered_date_dayofweek") > 0)
    .orderBy(F.col("avg_delivered_date_dayofweek").desc_nulls_last())
)
