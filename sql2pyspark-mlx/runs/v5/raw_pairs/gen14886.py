from pyspark.sql import functions as F

src = df_payments.filter(F.to_date("paid_at") < "2023-05-01")

method_group_summary = (
    src
    .groupBy(
        F.when(F.col("method").isin(["paypal", "card"]), "tier_1").otherwise("other").alias("method_group"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    method_group_summary
    .filter(F.col("avg_amount") > 680)
    .orderBy("avg_amount")
)
