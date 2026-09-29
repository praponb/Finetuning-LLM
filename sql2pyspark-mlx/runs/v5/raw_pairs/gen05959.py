from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("segment", "city").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.sum("age").over(w_recent).alias("cumulative_age"),
        F.row_number().over(w_running).alias("row_seq"),
        F.count("credit_limit").over(w3).alias("country_count_credit_limit"),
    )
)
