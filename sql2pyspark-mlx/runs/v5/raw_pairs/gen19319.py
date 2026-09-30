from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("method") == "paypal") & ~F.col("status").isin(["SUCCESS", "FAILED"])) | F.hour("paid_at").between(12, 20))
    .groupBy("method", F.year("paid_at").alias("paid_year"))
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 1300)
)
