from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("city").orderBy(F.col("age_scaled").asc_nulls_first())
w3 = Window.partitionBy("city", "segment").orderBy("age_scaled", "id").rowsBetween(-6, -1)

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-07-06"))) & F.col("city").isNotNull())
    .withColumn("age_scaled", F.round((F.col("age") * 1.1), 2))
    .withColumn("city_clean", F.coalesce("city", F.lit("N/A")))
)

result = (
    valid
    .filter(F.col("age") <= 33)
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("age_scaled_cume_dist", F.cume_dist().over(w2))
    .withColumn("age_avg_6_prior", F.avg("age").over(w3))
)
