from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.country").isin(["TH", "UK"]), "core").otherwise("other").alias("country_group"),
        F.when(F.col("c.age") >= 62, "high").when(F.col("c.age") >= 40, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.avg(F.col("o.discount")).alias("avg_discount"),
        F.sum(F.when(F.col("o.channel") == "mobile", F.col("o.shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
    )
    .orderBy("age_bucket", "country_group")
    .limit(20)
)
