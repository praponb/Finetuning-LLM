from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.orderBy("signup_date")

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB"]))
    .select(
        "id",
        "credit_limit",
        "country",
        "signup_date",
        F.first("country").over(w).alias("first_country"),
        F.dense_rank().over(w2).alias("signup_date_dense_rank"),
    )
)
