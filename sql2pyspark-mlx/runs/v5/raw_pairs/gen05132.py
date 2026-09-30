from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("min_credit_limit_by_country", F.min("credit_limit").over(w))
    .filter(F.col("credit_limit") == F.col("min_credit_limit_by_country"))
    .select("id", "country", "credit_limit", "segment")
    .orderBy("segment")
)
