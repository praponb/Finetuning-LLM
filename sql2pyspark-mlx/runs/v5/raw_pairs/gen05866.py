from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_year")

result = (
    df_customers
    .filter(F.col("age") != 52)
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("cumulative_age", F.sum("total_age").over(w))
)
