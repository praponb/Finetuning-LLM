from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(F.col("signup_date") < "2024-09-10")
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        "city",
        F.lead("age", 1).over(w).alias("next_age"),
        F.rank().over(w2).alias("signup_date_rank"),
    )
)
