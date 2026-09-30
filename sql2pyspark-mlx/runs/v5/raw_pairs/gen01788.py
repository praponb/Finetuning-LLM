from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 11, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("shipping_fee_bucket").desc())
)
