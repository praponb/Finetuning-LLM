from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("subscriptions.status").isin(["EXPIRED", "ACTIVE"]))
    .select(
        F.col("subscriptions.customer_id"),
        F.col("customers.credit_limit"),
        F.when(F.col("subscriptions.plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
    .orderBy(F.col("is_basic").desc(), F.col("credit_limit").desc())
)
