from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .select("id", "segment", "credit_limit", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
)
