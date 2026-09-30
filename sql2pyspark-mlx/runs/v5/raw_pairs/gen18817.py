from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .filter(F.month("signup_date").isin([4, 5, 7]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "country", "credit_limit", "email")
    .orderBy("credit_limit")
)
