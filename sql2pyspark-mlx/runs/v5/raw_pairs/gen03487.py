from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("age_rounded", "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("signup_date").desc(), "id")

filtered = (
    df_customers
    .filter(~F.col("city").isin(["London", "Singapore"]) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.round("age", 2).alias("age_rounded"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    filtered
    .filter(F.col("age_rounded") >= 58)
    .select(
        "id",
        "country",
        "signup_date",
        "city_filled",
        F.lag("age", 2).over(w).alias("prev_age"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
