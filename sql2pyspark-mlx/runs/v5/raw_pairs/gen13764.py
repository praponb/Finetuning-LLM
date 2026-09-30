from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isin([24, 31, 36, 39]))

agg = (
    filtered_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    agg
    .filter(F.col("balance_range") >= 36)
    .select("opened_cohort", "status_values", "balance_range")
)
