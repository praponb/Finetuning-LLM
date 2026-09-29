from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)
