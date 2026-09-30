from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "age",
        "country",
        "credit_limit",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_4"),
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
    )
    .orderBy("signup_date")
)
