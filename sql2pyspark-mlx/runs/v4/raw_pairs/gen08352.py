from pyspark.sql import functions as F

with_dims = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

result = (
    with_dims
    .groupBy("country")
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("country") == "US").alias("us_events"),
    )
)
