from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(~F.col("city").isin(["Bangkok", "Tokyo", "Chiang Mai"]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "city", "signup_date", "credit_limit")
    .orderBy("credit_limit")
)
