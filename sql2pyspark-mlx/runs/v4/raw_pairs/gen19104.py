from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round("amount", 2).alias("amount_rounded"),
        F.length("method").alias("method_length"),
    )
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("month", "paid_at"))
    .groupingSets(
        [["activity_month", "status"], ["activity_month"]],
        "activity_month",
        "status",
    )
    .agg(
        F.sum(F.when(F.col("method") == "cash", F.col("amount_rounded")).otherwise(0)).alias("cash_amount_rounded"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.countDistinct("method").alias("distinct_methods"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("cash_amount_rounded").asc_nulls_first())
)
