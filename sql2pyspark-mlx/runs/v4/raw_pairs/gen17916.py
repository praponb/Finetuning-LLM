from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .groupBy(F.col("c.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
    .filter(F.col("pct_grab") > 47)
    .orderBy(F.col("pct_grab").desc(), "country")
    .limit(10)
)
