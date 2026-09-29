from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_active")

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_credit_limit", F.sum("total_credit_limit").over(w))
)
