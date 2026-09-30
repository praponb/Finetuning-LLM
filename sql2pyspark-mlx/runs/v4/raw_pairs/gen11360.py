from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")
w2 = Window.orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("signup_date").desc(), "id")

enriched = (
    df_customers
    .select(
        "*",
        F.avg("age").over(w).alias("country_avg_age"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_6"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "country_avg_age",
        "credit_limit_avg_6",
    )
    .orderBy("credit_limit_avg_6", "city")
)
