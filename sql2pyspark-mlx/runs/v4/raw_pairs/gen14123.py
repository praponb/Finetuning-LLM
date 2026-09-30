from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.partitionBy("country", "city").orderBy("age", "id")
w5 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .withColumn("credit_limit_bucket", F.ntile(3).over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
    .withColumn("prev_age", F.lag("age", 2).over(w3))
    .withColumn("age_bucket", F.ntile(10).over(w4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w5))
)
