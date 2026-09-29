from pyspark.sql import functions as F

matched_keys = (
    df_shipments
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("carrier") == "UPS")
        .select("order_id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("cost") >= 105, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
    .filter(F.col("kerry_count") > 1)
    .orderBy(F.col("kerry_count").desc())
)
