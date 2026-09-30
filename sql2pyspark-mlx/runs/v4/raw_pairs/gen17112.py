from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-08-01")
    .select(
        "*",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

valid = (
    sessionized
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "status_prefix",
        "resolved_method",
        "new_group_flag",
        "burst_id",
        F.least("burst_id", "new_group_flag").alias("least_burst_id_new_group_flag"),
    )
)

cleaned = (
    valid
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "status_prefix",
        "resolved_method",
        "new_group_flag",
        "burst_id",
        F.col("new_group_flag").cast("string").alias("new_group_flag_num"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "method",
        "amount",
        "status_prefix",
        "resolved_method",
        "new_group_flag",
    )
    .orderBy(F.col("new_group_flag").desc_nulls_last())
)
