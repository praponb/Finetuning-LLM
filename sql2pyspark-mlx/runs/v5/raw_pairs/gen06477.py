from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 7) & F.col("store_id").isNotNull())
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1230, "high").when(F.col("amount") >= 250, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
    )
    .orderBy(F.col("pending_amount").desc())
)
