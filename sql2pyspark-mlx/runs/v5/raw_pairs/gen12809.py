from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        "segment",
        F.lag("credit_limit", 1, 0).over(w).alias("prev_credit_limit"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc(), "running_credit_limit")
)
