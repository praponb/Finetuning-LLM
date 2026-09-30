from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-06-03").cast("date")) & F.col("credit_limit").between(4600, 13000))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
    )
)

result = (
    valid
    .withColumn("cumulative_age_filled", F.sum("age_filled").over(w))
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w))
    .withColumn("city_min_credit_limit", F.min("credit_limit").over(w2))
)
