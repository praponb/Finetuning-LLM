from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last())
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("running_age", F.sum("age").over(w))
    .withColumn("credit_limit_rank", F.rank().over(w2))
    .withColumn("cumulative_age", F.sum("age").over(w3))
)
