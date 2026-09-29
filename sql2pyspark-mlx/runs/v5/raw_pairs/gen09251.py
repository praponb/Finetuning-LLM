from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("channel").eqNullSafe(F.lit("partner")))
    .select(
        "*",
        F.greatest("discount", "shipping_fee").alias("greatest_discount_shipping_fee"),
    )
)

result = (
    filtered
    .select(
        "id",
        "status",
        F.col("amount").alias("AMOUNT"),
        F.col("greatest_discount_shipping_fee").alias("GREATEST_DISCOUNT_SHIPPING_FEE_VAL"),
        F.col("discount").alias("DISCOUNT"),
    )
    .unpivot(
        ids=["id", "status"],
        values=["AMOUNT", "GREATEST_DISCOUNT_SHIPPING_FEE_VAL", "DISCOUNT"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
