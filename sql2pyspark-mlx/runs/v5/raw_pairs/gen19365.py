from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("signup_date")
)
