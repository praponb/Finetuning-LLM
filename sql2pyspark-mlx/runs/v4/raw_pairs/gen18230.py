from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "age",
        F.row_number().over(w).alias("rn"),
        F.first("signup_date").over(w2).alias("first_signup_date"),
    )
    .orderBy("signup_date")
)
