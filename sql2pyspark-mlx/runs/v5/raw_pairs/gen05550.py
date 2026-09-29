from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .groupBy("account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .filter(F.col("credit_count") < 77)
    .orderBy("account_type", "status_values")
    .limit(20)
)
