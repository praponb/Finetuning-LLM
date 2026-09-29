from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("pct_of_total", F.round(((F.col("total_age") * 100) / F.sum("total_age").over(w)), 2))
    .orderBy(F.col("signup_month").desc(), F.col("pct_of_total").desc())
)
