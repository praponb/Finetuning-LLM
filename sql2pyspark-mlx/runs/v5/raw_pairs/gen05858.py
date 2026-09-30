from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .groupBy("channel")
    .agg(F.countDistinct("shipping_fee").alias("unique_statuss"))
    .filter(F.col("unique_statuss") > 9)
    .select("channel", "unique_statuss")
)
