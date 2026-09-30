from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")
w2 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.lag("credit_limit", 1, 0).over(w).alias("prev_credit_limit"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)
