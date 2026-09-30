from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-10-03 13:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.upper("method").alias("method_upper"),
    )
)

cleaned = (
    base
    .filter((F.col("paid_at") > F.lit("2023-11-17").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "paid_at_label",
        F.upper("method").alias("method_upper_2"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

kept_keys = (
    cleaned
    .select("order_id", "status")
    .exceptAll(
        df_payments
        .filter(F.col("status") == "SUCCESS")
        .select("order_id", "status")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

cleaned_payments = (
    verified
    .filter(F.col("method").isNotNull())
    .select(
        "method",
        "paid_at",
        "status",
        "paid_at_label",
        "method_upper_2",
        "paid_at_day",
        "resolved_method",
        F.concat("status", F.lit(" "), "method").alias("status_method"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

result = (
    cleaned_payments
    .cube("method", "status")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("paid_at").alias("ended_at"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.grouping("method").alias("gid"),
    )
    .orderBy(F.col("refunded_rows").desc_nulls_first())
)
