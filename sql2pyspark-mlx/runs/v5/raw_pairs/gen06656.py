from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.sum("credit_limit").over(w_grp).alias("last4_sum_credit_limit"),
        F.min("credit_limit").over(w_grp).alias("last4_min_credit_limit"),
        F.count("credit_limit").over(w_grp).alias("last4_count_credit_limit"),
    )
)
