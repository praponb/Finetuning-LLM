from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([1, 11, 32]))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
