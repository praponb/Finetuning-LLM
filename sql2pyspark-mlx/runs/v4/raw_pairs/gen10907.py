from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("credit_limit").between(4800, 8800))
    .select("id", "country", "age", F.row_number().over(w).alias("row_num"))
    .orderBy("id")
)
