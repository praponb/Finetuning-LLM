from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_customers
    .filter(F.col("signup_date") < "2023-06-01")
    .select("id", "country", "age", F.min("age").over(w).alias("country_min_age"))
)
