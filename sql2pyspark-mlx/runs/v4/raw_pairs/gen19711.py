from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
