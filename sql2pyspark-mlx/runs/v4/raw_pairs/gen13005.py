from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([5, 9, 30, 36]) & (F.col("opened_date") > "2023-04-16"))
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("customer_id").desc())
)
