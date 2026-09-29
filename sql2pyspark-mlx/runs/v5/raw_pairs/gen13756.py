from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1230, "high").when(F.col("amount") >= 450, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 62)
)
