from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

base = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-06-20")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    base
    .select(
        "*",
        F.avg("age").over(w).alias("age_avg_6"),
        F.ntile(5).over(w2).alias("age_bucket"),
    )
)
