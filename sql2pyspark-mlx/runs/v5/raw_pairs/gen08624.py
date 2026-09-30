from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("method").isin(["card", "bank_transfer"]), "group_a").otherwise("other").alias("method_group"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") < 6)
    .orderBy("total_count")
)
