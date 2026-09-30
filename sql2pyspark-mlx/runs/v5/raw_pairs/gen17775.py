from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-04-01", "2024-03-01"))
    .select("id", "city", "age", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "city", "age")
)
