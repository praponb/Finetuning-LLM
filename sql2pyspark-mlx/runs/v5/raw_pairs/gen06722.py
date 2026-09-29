from pyspark.sql import functions as F

with_dims = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.country"), F.col("c.signup_date"), F.col("c.segment"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("balance") >= 63000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "status",
    )
    .agg(
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") > 0)
    .orderBy(F.col("balance_tier").desc_nulls_first())
)
