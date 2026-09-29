from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-08-26")))
    .withColumn("least_amount_shipping_fee", F.least("amount", "shipping_fee"))
    .withColumn("discount_band", F.when(F.col("discount") >= 0.43, "High").when(F.col("discount") >= 0.12, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .select("id", "customer_id", "discount", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "customer_id"],
        values=["discount", "amount", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
