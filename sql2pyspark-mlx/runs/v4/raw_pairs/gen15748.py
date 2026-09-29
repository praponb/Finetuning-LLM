from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-08-18")
    .select(
        "id",
        "cost",
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("quarter", "delivered_date_period").alias("quarter_start"))
    .agg(
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("span_minutes"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
