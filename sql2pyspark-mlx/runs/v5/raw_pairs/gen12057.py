from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("t.merchant").isin(["Shell", "Grab", "Lazada"]))
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .orderBy("pct_lazada")
)
