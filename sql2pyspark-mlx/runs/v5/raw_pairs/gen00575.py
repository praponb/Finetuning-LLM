from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.partitionBy("status", "segment").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("YEAR", "end_date").alias("end_date_period"),
        F.when(F.col("monthly_fee") >= 270, "High").when(F.col("monthly_fee") >= 97, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country").isin(["UK", "DE", "JP"]))
    .select("a.*", F.col("b.email"), F.col("b.segment"))
)

scored = (
    with_dims
    .withColumn("monthly_fee_bucket", F.ntile(4).over(w))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
    .withColumn("monthly_fee_avg_2", F.avg("monthly_fee").over(w3))
)

matched_keys = (
    scored
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
        .select("customer_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("plan", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id") == 2)
        .select("plan", "status")
    )
)

kept_rows_subscriptions = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows_subscriptions
    .filter((~F.col("segment").eqNullSafe(F.lit("Corporate"))) & F.col("customer_id").isin([4, 6, 7, 32]))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "end_date_period",
        "monthly_fee_level",
        "email",
        "segment",
        "monthly_fee_bucket",
        "monthly_fee_avg_2",
        F.round((F.col("monthly_fee_avg_2") / F.col("monthly_fee_rank")), 2).alias("monthly_fee_avg_2_per_monthly_fee_rank"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

result = (
    cleaned
    .groupBy("customer_id", F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("monthly_fee_avg_2") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("monthly_fee_avg_2_per_smb"),
    )
)
