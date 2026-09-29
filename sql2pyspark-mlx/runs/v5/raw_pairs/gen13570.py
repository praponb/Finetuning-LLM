from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.lag("age").over(w_grp).alias("prev_age"),
        F.lead("age").over(w_grp).alias("next_age"),
        F.lead("credit_limit").over(w_grp).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_grp).alias("prev_credit_limit"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)
