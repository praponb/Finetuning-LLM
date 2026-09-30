from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("age") < 31)
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        "city",
        F.round(F.avg("credit_limit").over(w), 2).alias("credit_limit_moving_avg_7"),
        F.row_number().over(w2).alias("rn"),
    )
)
