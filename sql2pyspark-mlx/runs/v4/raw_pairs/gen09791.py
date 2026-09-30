from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.when(F.col("c.country").isin(["TH", "SG"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(
        F.max(F.col("o.discount")).alias("max_discount"),
        F.min(F.col("o.amount")).alias("min_amount"),
    )
    .orderBy("max_discount")
)
