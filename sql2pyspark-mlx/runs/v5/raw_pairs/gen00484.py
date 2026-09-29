from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment", "country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .withColumn("next_age", F.lead("age").over(w))
    .withColumn("prev_age", F.lag("age").over(w))
    .withColumn("next_credit_limit", F.lead("credit_limit").over(w))
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w2))
)
