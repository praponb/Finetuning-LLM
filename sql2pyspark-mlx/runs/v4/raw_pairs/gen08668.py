from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("method").isin(["promptpay", "bank_transfer"]), "core").otherwise("other").alias("method_group"),
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
