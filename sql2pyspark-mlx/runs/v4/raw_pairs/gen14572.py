from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.lower("status").alias("status_lower"),
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("balance") >= 62000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list_2"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list_3"),
    )
    .filter(F.col("credit_balance") > 0)
    .orderBy(F.col("credit_balance").desc_nulls_first())
)
