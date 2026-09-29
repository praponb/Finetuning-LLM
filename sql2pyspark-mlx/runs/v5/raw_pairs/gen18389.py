from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 11)
    .groupBy(
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("first_opened_date")
)
