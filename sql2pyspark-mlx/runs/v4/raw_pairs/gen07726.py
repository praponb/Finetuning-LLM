from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("amount_tier", F.when(F.col("amount") >= 1300, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["amount_tier", "method"], ["amount_tier"], []],
        "amount_tier",
        "method",
    )
    .agg(
        F.min("paid_at").alias("started_at"),
        F.count("*").alias("num_rows"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
