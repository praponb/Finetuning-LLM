from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
