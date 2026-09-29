from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "items",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("cart_value") >= 1610, "HIGH").otherwise("LOW").alias("cart_value_tier"),
    )
    .agg(
        F.round(F.avg("cart_value"), 2).alias("avg_cart_value"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
    )
    .orderBy(F.col("cart_value_tier").desc())
)
