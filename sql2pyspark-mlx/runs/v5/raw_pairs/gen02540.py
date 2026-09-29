from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB"]))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("credit_limit_rank", F.rank().over(w))
)
