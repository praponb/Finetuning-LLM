from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit", "id")
w4 = Window.partitionBy("country", "segment")

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-09-06")))
    .select(
        "*",
        F.lower("segment").alias("segment_lower"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        "age",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.lag("credit_limit", 1).over(w2).alias("prev_credit_limit"),
        F.ntile(5).over(w3).alias("credit_limit_bucket"),
        F.corr("credit_limit", "age").over(w4).alias("corr_credit_limit_age"),
    )
)
