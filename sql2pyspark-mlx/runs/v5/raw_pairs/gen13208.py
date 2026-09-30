from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
    .orderBy(F.col("paid_year").desc())
)
