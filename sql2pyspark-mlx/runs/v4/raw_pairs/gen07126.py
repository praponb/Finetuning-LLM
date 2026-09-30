from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-07").cast("date")) & (F.col("credit_limit") >= 8800))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") * 1.07), 2).alias("credit_limit_scaled"),
        F.date_add("signup_date", 4).alias("signup_date_plus_4d"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
