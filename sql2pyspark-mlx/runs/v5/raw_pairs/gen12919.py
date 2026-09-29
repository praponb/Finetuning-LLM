from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "credit_limit",
        F.lag("credit_limit").over(w_running).alias("prev_credit_limit"),
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("age").over(w_running).alias("next_age"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)
