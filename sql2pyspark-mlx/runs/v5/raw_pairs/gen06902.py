from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "DAVID LEE")
    .select(
        "id",
        "name",
        "email",
        "is_active",
        "age",
        F.max("credit_limit").over(w).alias("last4_max_credit_limit"),
        F.avg("credit_limit").over(w).alias("last4_avg_credit_limit"),
        F.sum("credit_limit").over(w).alias("last4_sum_credit_limit"),
    )
)
