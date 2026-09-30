from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("cnt").desc())

result = (
    df_payments
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("amount") >= 1450, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("paid_at").alias("started_at"),
        F.max("paid_at").alias("last_seen"),
    )
    .withColumn("cnt_rank", F.rank().over(w))
    .orderBy(F.col("amount_tier").desc_nulls_first())
)
