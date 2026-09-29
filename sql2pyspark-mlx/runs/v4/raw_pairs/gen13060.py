from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("discount") < 0.24)
    .withColumn("status_lower", F.lower("status"))
)

result = (
    valid
    .select(
        "id",
        "channel",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("amount").alias("AMOUNT"),
        F.col("discount").alias("DISCOUNT"),
    )
    .unpivot(
        ["id", "channel"],
        ["SHIPPING_FEE_VAL", "AMOUNT", "DISCOUNT"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
