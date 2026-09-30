from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("greatest_amount_shipping_fee", F.greatest("amount", "shipping_fee"))
)

result = (
    valid
    .select(
        "id",
        "status",
        F.col("shipping_fee").alias("SHIPPING_FEE"),
        "greatest_amount_shipping_fee",
    )
    .unpivot(
        ids=["id", "status"],
        values=["SHIPPING_FEE", "greatest_amount_shipping_fee"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
