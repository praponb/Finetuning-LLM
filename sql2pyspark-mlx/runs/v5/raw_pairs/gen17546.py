from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc())

result = (
    df_customers
    .select("id", "country", "age", "name", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "country", "age", "name")
    .orderBy("country")
)
