from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city", "country")

base = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-08-28"))) & (F.col("credit_limit") <= 5800))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    base
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        F.max("age").over(w).alias("last3_max_age"),
        F.avg("age").over(w).alias("last3_avg_age"),
        F.sum("age").over(w).alias("last3_sum_age"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)
