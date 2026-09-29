from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("age") <= 46)
    .select(
        "id",
        "city",
        "age",
        "segment",
        "signup_date",
        "credit_limit",
        F.row_number().over(w).alias("rn"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
)
