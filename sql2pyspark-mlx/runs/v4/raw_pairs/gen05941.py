from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first())
w5 = Window.partitionBy("status", "method").orderBy(F.col("amount").desc_nulls_last())

kept_keys = (
    df_payments
    .select("status", "method")
    .intersect(
        df_payments
        .filter(F.to_date("paid_at") < "2024-04-04")
        .select("status", "method")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.max("amount").over(w).alias("amount_max_30d"),
        F.rank().over(w2).alias("amount_rank"),
        F.max("amount").over(w3).alias("amount_max_30d_2"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.cume_dist().over(w5).alias("amount_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("amount_max_30d_2") == 907)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_max_30d",
        "amount_rank",
        (F.col("amount") * F.col("amount_max_30d_2")).alias("amount_times_amount_max_30d_2"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

filtered = (
    cleaned
    .filter(F.col("order_id").isNotNull() & (F.col("amount_times_amount_max_30d_2") < 44))
    .select(
        "id",
        "paid_at",
        "status",
        "amount_max_30d",
        "amount_rank",
        "amount_times_amount_max_30d_2",
        "resolved_method",
        F.trim("method").alias("clean_method"),
        F.floor("amount_times_amount_max_30d_2").alias("amount_times_amount_max_30d_2_whole"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method_2"),
    )
)

valid = (
    filtered
    .filter(F.col("paid_at") > F.lit("2023-06-21").cast("date"))
    .withColumn("amount_times_amount_max_30d_2_whole_num", F.col("amount_times_amount_max_30d_2_whole").cast("double"))
    .withColumn("paid_at_period", F.date_trunc("MONTH", "paid_at"))
)

result = (
    valid
    .select(
        "id",
        "status",
        "amount_max_30d",
        "amount_rank",
        "resolved_method",
        "clean_method",
        "amount_times_amount_max_30d_2_whole",
        "resolved_method_2",
        "paid_at_period",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "GROUP_B").when(F.col("status").rlike("^(SUCCESS)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("status").desc())
)
