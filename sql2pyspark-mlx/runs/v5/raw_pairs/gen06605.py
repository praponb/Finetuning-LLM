from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_last(), "id")

latest = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .withColumn("amount_tier", F.when(F.col("amount") >= 1560, "LARGE").otherwise("SMALL"))
    .cube("amount_tier", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "promptpay", 1)), F.lit(0)), 2).alias("amount_per_promptpay"),
        F.grouping("amount_tier").alias("grouping_id"),
    )
    .filter(F.col("failed_events") > 1)
)
