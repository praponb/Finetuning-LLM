from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at_hour").desc_nulls_first(), "id")

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

cleaned = (
    filtered
    .filter(F.col("amount") >= 530)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "paid_at_hour",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

base = (
    cleaned
    .filter(F.col("amount").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        "paid_at_hour",
        F.to_date("paid_at").alias("paid_at_date"),
        F.round((F.col("amount") * 1.15), 2).alias("adjusted_amount"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "order_id",
        "status",
        "paid_at_hour",
        "paid_at_date",
        "adjusted_amount",
        "rn",
    )
    .orderBy("id")
    .limit(20)
)
