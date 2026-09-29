from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-01-01", "2025-10-01"))
    .agg(
        F.count("balance").alias("balance_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)
