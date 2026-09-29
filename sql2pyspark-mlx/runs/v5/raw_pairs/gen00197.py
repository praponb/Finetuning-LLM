from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")

latest = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "name", "email", "country", "segment", "is_active", "age", "rn")
    .orderBy(F.col("segment").asc_nulls_first())
)
