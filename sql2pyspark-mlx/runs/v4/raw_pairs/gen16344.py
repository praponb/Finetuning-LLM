from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-07-29").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "age",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.concat("country", F.lit(" - "), "city").alias("country_city"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
