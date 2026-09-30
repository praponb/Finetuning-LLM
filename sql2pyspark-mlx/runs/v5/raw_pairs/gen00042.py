from pyspark.sql import functions as F

src = df_payments.filter((F.col("amount") <= 1570) & (F.col("amount") < 1170))

agg = (
    src
    .groupBy(F.hour("paid_at").alias("paid_hour"), "method")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = agg.filter(F.col("stddev_amount") >= 24)
