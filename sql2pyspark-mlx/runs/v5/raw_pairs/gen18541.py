from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("max_age_by_country", F.max("age").over(w))
    .filter(F.col("age") == F.col("max_age_by_country"))
    .select("id", "country", "age", "credit_limit")
    .orderBy(F.col("credit_limit").desc())
)
