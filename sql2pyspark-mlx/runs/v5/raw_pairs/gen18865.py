from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .groupBy("is_active")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("rnk").desc())
)
