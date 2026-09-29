from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .filter(F.col("credit_limit") <= 12600)
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_3"),
    )
    .orderBy("signup_date")
)
