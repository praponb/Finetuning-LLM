from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select("id", "segment", "age", "credit_limit", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "segment", "age", "credit_limit")
    .orderBy("age", F.col("credit_limit").desc())
)
