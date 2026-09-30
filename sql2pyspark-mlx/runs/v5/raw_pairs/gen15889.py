from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "name", "email", "city", "signup_date", "is_active", "age")
)

result = (
    latest
    .select("id", "city", "signup_date", "is_active", "age")
    .orderBy(F.col("id").asc_nulls_last())
)
