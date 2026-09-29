from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-09-18").cast("date"))
    .select(
        "*",
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date_period")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") >= 20), "left_anti")
)

result = (
    eligible
    .cube(F.date_trunc("WEEK", "opened_date_period").alias("week_start"))
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("sqft_per_north"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
)
