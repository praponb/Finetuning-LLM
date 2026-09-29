from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.upper("segment").alias("segment_upper"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

result = (
    cleaned
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w))
    .withColumn("age_avg_5", F.avg("age").over(w2))
)
