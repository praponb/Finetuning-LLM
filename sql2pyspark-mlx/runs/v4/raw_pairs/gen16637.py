from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
    .orderBy(F.col("shipped_month").desc())
)
