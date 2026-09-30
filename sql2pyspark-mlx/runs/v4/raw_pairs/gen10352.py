from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select("id", "city", "signup_date", "email", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "city", "signup_date", "email")
    .orderBy(F.col("id").desc(), F.col("city").desc())
)
