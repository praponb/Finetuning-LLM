from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .select("id", "country", "age", "signup_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
)
