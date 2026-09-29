from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.count("credit_limit").over(w).alias("last3_count_credit_limit"),
        F.sum("credit_limit").over(w).alias("last3_sum_credit_limit"),
        F.min("credit_limit").over(w).alias("last3_min_credit_limit"),
    )
)
