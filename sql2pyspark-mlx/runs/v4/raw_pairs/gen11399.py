from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age")

result = (
    df_customers
    .select("id", "city", "age", F.dense_rank().over(w).alias("age_dense_rank"))
    .orderBy(F.col("age").desc())
)
