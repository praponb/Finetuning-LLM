from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date")

result = (
    df_customers
    .select("id", "signup_date", F.dense_rank().over(w).alias("signup_date_dense_rank"))
)
