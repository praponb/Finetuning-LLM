from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("credit_limit_rank", F.rank().over(w))
    .orderBy("credit_limit_rank")
)
