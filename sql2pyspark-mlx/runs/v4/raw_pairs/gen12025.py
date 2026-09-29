from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") < "2024-03-01")
    .select("*", F.upper("country").alias("country_upper"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
