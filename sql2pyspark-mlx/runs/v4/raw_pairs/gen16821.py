from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first())
w3 = Window.orderBy(F.col("age").asc_nulls_first())
w4 = Window.partitionBy("city").orderBy("age", "id")
w5 = Window.orderBy("credit_limit", "id").rowsBetween(-3, -1)

with_windows = (
    df_customers
    .withColumn("next_age", F.lead("age", 2, 0).over(w))
    .withColumn("age_percent_rank", F.percent_rank().over(w2))
    .withColumn("age_cume_dist", F.cume_dist().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("credit_limit_avg_3_prior", F.avg("credit_limit").over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "next_age",
        "age_percent_rank",
    )
    .orderBy(F.col("next_age").desc(), F.col("id").desc())
    .limit(10)
)
