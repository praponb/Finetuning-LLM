from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["partner", "web"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_amount") * 100) / F.sum("total_amount").over(w)), 2))
    .orderBy(F.col("channel_group").desc())
)
