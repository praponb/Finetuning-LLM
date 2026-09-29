from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.lag("credit_limit", 1).over(w).alias("prev_credit_limit"),
    )
    .orderBy("prev_credit_limit")
)
