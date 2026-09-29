from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("credit_limit", "id")

result = (
    df_customers
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "age",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
