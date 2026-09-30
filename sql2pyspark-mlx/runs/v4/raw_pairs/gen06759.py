from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

metrics = (
    df_payments
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_bucket", F.ntile(3).over(w2))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.when(F.col("amount") >= 1270, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("amount_bucket") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("amount_bucket_per_card"),
        F.min("paid_at").alias("first_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
