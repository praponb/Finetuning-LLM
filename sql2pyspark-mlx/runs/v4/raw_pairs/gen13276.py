from pyspark.sql import functions as F

remaining_keys = (
    df_payments
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([8, 11, 15, 30]))
        .select("order_id", "status")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .filter(F.col("amount_p50") > 0)
    .orderBy(F.col("duration_min").desc_nulls_first())
)
