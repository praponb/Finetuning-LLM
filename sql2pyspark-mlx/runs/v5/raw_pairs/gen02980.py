from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([9, 11, 12]))
    .groupBy("status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        F.countDistinct("shipping_fee").alias("unique_stores"),
    )
    .filter(F.col("unique_statuss") < 3)
    .select("status", "unique_statuss", "unique_stores")
    .orderBy("status")
    .limit(5)
)
