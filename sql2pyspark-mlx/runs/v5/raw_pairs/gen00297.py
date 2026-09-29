from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "segment", "signup_date", "age")
)
