from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last(), "id")

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
    )
    .orderBy(F.col("country").desc())
)
