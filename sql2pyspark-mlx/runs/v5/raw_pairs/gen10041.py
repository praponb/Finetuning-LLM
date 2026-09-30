from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.col("age").cast("string").alias("age_num"),
    )
)

result = (
    base
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        F.count("age").over(w_hist).alias("running_count_age"),
        F.max("age").over(w_hist).alias("running_max_age"),
        F.avg("age").over(w_hist).alias("running_avg_age"),
    )
)
