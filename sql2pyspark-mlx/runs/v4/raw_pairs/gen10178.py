from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("amount") <= 630)
    .select(
        "*",
        F.lower("status").alias("status_lower"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

result = (
    valid
    .rollup("order_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_amount") > 0)
)
