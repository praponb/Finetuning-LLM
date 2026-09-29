from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
    .filter(F.col("card_amount") > 22)
    .select("paid_day", "card_amount")
    .orderBy("paid_day")
)
