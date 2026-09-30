from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "partner") | F.col("channel").isin(["mobile", "partner", "store"]))
    .groupBy(
        F.when(F.col("discount") >= 0.43, "high").when(F.col("discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.min("amount").alias("min_amount"))
)
