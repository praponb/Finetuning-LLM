from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("status").desc())
)
