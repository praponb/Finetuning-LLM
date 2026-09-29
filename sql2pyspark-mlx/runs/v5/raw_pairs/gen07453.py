from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("is_new_york")
w2 = Window.partitionBy("segment").orderBy(F.col("is_new_york").desc(), "id")
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.partitionBy("city", "segment").orderBy("age", "id").rowsBetween(-3, -1)

valid = (
    df_customers
    .filter((F.col("segment") == "Corporate") & (F.col("signup_date") > F.to_date(F.lit("2023-09-21"))))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.coalesce("name", F.lit("Unknown")).alias("name_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "country",
        "city",
        "segment",
        F.cume_dist().over(w).alias("is_new_york_cume_dist"),
        F.ntile(3).over(w2).alias("is_new_york_bucket"),
        F.lead("age", 1).over(w3).alias("next_age"),
        F.ntile(4).over(w4).alias("credit_limit_bucket"),
        F.avg("credit_limit").over(w5).alias("credit_limit_avg_3_prior"),
    )
)
