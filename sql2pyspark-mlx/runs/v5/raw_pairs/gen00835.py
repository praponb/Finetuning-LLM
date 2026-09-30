from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") >= 68000)
    .select("customer_id", "status_values", "min_balance")
)
