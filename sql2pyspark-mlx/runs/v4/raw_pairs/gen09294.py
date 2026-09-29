from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")
w2 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "segment").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("segment", "city")

result = (
    df_customers
    .withColumn("age_bucket", F.ntile(10).over(w))
    .withColumn("cumulative_age", F.sum("age").over(w2))
    .withColumn("age_bucket_2", F.ntile(5).over(w))
    .withColumn("age_bucket_3", F.ntile(5).over(w3))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4))
)
