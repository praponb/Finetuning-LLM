from pyspark.sql import functions as F

src = df_payments.filter(F.col("status") != "FAILED")

payments_stats = (
    src
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)

result = payments_stats.orderBy("status")
