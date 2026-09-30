from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country", "segment")
w3 = Window.partitionBy("city").orderBy("age", "id")
w4 = Window.partitionBy("country")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-12-19"))) & F.col("segment").isNotNull())
    .select(
        "*",
        F.add_months("signup_date", 6).alias("signup_date_plus_months"),
        F.date_trunc("month", "signup_date").alias("signup_date_period"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

result = (
    cleaned
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("country_max_age", F.max("age").over(w4))
)
