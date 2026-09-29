from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("opened_date") >= "2024-06-02"))
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
