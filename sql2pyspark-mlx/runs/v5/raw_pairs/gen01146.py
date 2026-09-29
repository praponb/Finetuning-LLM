from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .filter(F.upper(F.trim("email")) == "FRANK@COMPANY.CO")
    .select(
        "id",
        "city",
        "signup_date",
        "age",
        F.round(F.avg("age").over(w), 2).alias("age_moving_avg_3"),
    )
)
