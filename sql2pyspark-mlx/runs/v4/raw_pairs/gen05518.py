from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.upper("country").alias("country_upper"),
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date"))
)
