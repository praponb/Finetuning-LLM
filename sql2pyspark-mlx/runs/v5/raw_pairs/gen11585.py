from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("name").like("He%"))
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "country", "signup_date", "credit_limit")
)
