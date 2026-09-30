from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("age") >= 46)
    .select("id", "segment", "age", F.max("age").over(w).alias("segment_max_age"))
    .orderBy("id")
)
