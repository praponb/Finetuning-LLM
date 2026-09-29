from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_quarter")

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_credit_limit", F.sum("total_credit_limit").over(w))
)
