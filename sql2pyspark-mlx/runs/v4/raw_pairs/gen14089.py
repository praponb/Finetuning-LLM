from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status").isNotNull())
    .groupBy(
        F.when(F.col("c.country").isin(["TH", "US"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"))
    .orderBy("country_group")
)
