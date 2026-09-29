from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first(), "id")

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "name", "email", "segment", "is_active", "credit_limit")
)

result = latest.select("id", "name", "email", "segment", "credit_limit")
