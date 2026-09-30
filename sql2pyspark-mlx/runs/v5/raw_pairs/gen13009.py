from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("age") >= 42)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
    .orderBy(F.col("id").desc())
)
