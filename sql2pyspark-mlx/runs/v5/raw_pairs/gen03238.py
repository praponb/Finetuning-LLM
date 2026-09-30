from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .groupBy(
        F.when(F.col("method").isin(["cash", "paypal"]), "group_a").otherwise("other").alias("method_group"),
    )
    .agg(F.countDistinct("amount").alias("unique_methods"))
    .filter(F.col("unique_methods") > 10)
    .select("method_group", "unique_methods")
)
