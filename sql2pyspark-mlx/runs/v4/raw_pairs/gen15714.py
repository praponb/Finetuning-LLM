from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select("id", "city", "credit_limit", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "city", "credit_limit")
    .orderBy("city")
)
