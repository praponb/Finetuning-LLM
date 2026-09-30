from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")

metrics = (
    df_customers
    .select(
        "*",
        F.lag("age").over(w).alias("prev_age"),
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "prev_age",
        "next_credit_limit",
        F.when(F.col("city").rlike("^(New York|Bangkok)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").asc_nulls_last())
)
