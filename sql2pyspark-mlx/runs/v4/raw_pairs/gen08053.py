from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first())
w4 = Window.partitionBy("segment").orderBy("corr_credit_limit_age", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("country", "city").orderBy(F.col("age").asc_nulls_first())
w6 = Window.partitionBy("city").orderBy("corr_credit_limit_age", "id").rowsBetween(-2, -1)

scored = (
    df_customers
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    latest
    .filter((F.col("signup_date") >= F.lit("2024-03-10").cast("date")) & (F.col("corr_credit_limit_age") > 0.5))
    .withColumn("corr_age_credit_limit_band", F.when(F.col("corr_age_credit_limit") >= 0.39, "High").when(F.col("corr_age_credit_limit") >= -0.8, "Medium").otherwise("Low"))
)

metrics = (
    valid
    .withColumn("corr_credit_limit_age_avg_6_prior", F.avg("corr_credit_limit_age").over(w4))
    .withColumn("age_dense_rank", F.dense_rank().over(w5))
    .withColumn("corr_age_credit_limit_avg_2_prior", F.avg("corr_age_credit_limit").over(w6))
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "corr_credit_limit_age",
    )
    .orderBy(F.col("credit_limit").desc(), "id")
)
