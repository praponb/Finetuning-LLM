from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_success").desc_nulls_last())
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("is_success_filled_count_7d_tier").orderBy(F.col("num_rows").desc())

base = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") < "2024-10-01"))
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
)

base_payments = (
    base
    .filter(F.col("paid_at") >= F.lit("2023-08-05").cast("date"))
    .select(
        "id",
        "paid_at",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)

cleaned = (
    base_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-14").cast("date")) & F.hour("paid_at").between(7, 15))
    .withColumn("is_success_filled", F.coalesce("is_success", F.lit(0)))
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([2, 3, 13, 28]))
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("is_success_dense_rank", F.dense_rank().over(w))
    .withColumn("is_success_filled_count_7d", F.count("is_success_filled").over(w2))
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("is_success_filled_count_7d") >= 16, "LARGE").otherwise("SMALL").alias("is_success_filled_count_7d_tier"),
        F.date_trunc("month", "paid_at").alias("activity_month"),
        F.date_trunc("year", "paid_at").alias("year_start"),
    )
    .agg(
        F.round(F.avg("is_success_filled_count_7d"), 2).alias("avg_is_success_filled_count_7d"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
        F.max("paid_at").alias("last_paid_at"),
        F.avg("is_success_filled_count_7d").alias("avg_is_success_filled_count_7d_2"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w3))
)

result = (
    totals
    .select(
        "is_success_filled_count_7d_tier",
        "activity_month",
        "year_start",
        "avg_is_success_filled_count_7d",
        "duration_min",
        "num_rows",
        "avg_is_success_filled_count_7d_2",
    )
    .orderBy(F.col("is_success_filled_count_7d_tier").asc_nulls_last())
)
