from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("age", "id")
w2 = Window.partitionBy("segment")

valid = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("Corporate"))) & (F.col("signup_date") > F.lit("2023-09-01").cast("date")))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.add_months("signup_date", 3).alias("signup_date_plus_months"),
        (F.col("credit_limit") - F.col("age")).alias("credit_limit_minus_age"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "age",
        "credit_limit_minus_age",
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lead("credit_limit_minus_age").over(w).alias("next_credit_limit_minus_age"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("credit_limit").over(w2).alias("segment_sum_credit_limit"),
    )
)
