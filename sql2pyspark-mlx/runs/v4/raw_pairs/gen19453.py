from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("discount")).otherwise(0)).alias("returned_discount"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .filter(F.col("partner_count") >= 56)
    .orderBy("status")
)
