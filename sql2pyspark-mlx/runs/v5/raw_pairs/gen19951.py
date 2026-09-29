from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("paid_at_hour").desc(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at_hour", "id")
w3 = Window.partitionBy("status").orderBy(F.col("event_seq").desc_nulls_first(), "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id")

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-11-12 17:00:00").cast("timestamp"))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.hour("paid_at").alias("paid_at_hour"),
        F.substring("status", 1, 3).alias("status_prefix"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.ntile(10).over(w).alias("paid_at_hour_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("paid_at_hour").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "paid_at_hour",
        "status_prefix",
        "method_filled",
        "paid_at_hour_bucket",
        "event_seq",
        F.to_date("paid_at").alias("paid_at_date"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "amount",
        "status",
        "status_prefix",
        "paid_at_hour_bucket",
        "event_seq",
        "paid_at_date",
    )
)

result = (
    deduped
    .select(
        "id",
        "amount",
        "status",
        "status_prefix",
        "paid_at_hour_bucket",
        "event_seq",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "EXTENDED").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("amount").asc_nulls_first())
)
