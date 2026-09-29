from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_month")

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("cumulative_credit_limit", F.sum("total_credit_limit").over(w))
)
