from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.when(F.col("amount") >= 1670, "high").when(F.col("amount") >= 710, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)
