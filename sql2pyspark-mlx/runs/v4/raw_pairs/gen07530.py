from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 690) & ((F.col("discount") < 0.29) | (F.col("shipping_fee") < 33)))
    .groupBy(
        "customer_id",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.sum("discount").alias("total_discount"),
    )
)
