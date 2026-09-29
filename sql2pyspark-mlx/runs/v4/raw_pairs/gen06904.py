from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.count("*").over(w).alias("city_count"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
)
