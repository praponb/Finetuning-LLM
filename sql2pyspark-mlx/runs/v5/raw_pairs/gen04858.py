from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(570, 1150))
    .groupBy(
        "status",
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.avg("amount").alias("avg_amount"),
    )
)
