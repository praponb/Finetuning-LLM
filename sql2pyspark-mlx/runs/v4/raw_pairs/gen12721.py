from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "credit_limit",
        "segment",
        "signup_date",
        "age",
        F.avg("credit_limit").over(w).alias("country_avg_credit_limit"),
        F.lag("age", 1).over(w2).alias("prev_age"),
    )
)
