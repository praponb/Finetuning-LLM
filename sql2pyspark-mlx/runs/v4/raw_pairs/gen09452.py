from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
