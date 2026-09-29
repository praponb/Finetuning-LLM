from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city", "segment").orderBy("age", "id")
w_running_2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("credit_limit") < 7000)
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.trim("country").alias("clean_country"),
    )
)

result = (
    base
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "age",
        "country_prefix",
        F.lead("age").over(w_running).alias("next_age"),
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("credit_limit").over(w_running).alias("next_credit_limit"),
        F.row_number().over(w_running).alias("event_seq"),
        F.last("age", ignorenulls=True).over(w_running_2).alias("last_known_age"),
    )
)
