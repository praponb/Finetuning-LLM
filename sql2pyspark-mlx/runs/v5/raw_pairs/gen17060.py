from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.lead("credit_limit").over(w_hist).alias("next_credit_limit"),
        F.lead("age").over(w_hist).alias("next_age"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.count("credit_limit").over(w2).alias("city_count_credit_limit"),
    )
)
