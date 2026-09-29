from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select("id", "country", "signup_date", "age", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "country", "signup_date", "age")
    .orderBy(F.col("age").desc())
)
