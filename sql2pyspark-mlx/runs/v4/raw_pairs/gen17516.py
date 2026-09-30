from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

remaining_keys = (
    flattened
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("created_at") >= F.lit("2025-03-02 08:00:00").cast("timestamp"))
    .withColumn("element_lower", F.lower("element"))
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.segment"))
)

result = (
    joined
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("hour_window").asc_nulls_first())
)
