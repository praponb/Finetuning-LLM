from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("max_credit_limit_by_country", F.max("credit_limit").over(w))
    .filter(F.col("credit_limit") == F.col("max_credit_limit_by_country"))
    .select("id", "country", "credit_limit", "city")
)
