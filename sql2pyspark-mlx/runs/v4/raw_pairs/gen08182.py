from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_credit_limit") * 100) / F.sum("total_credit_limit").over(w)), 2))
    .orderBy("city", F.col("total_credit_limit").desc())
)
