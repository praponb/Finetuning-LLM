from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "age",
        "segment",
        "credit_limit",
        F.first("age").over(w).alias("first_age"),
        F.row_number().over(w2).alias("credit_limit_row"),
    )
)
