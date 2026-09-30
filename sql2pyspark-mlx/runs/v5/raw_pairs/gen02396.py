from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("city").orderBy("signup_date", "id")
w3 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-09-15").cast("date"))
    .withColumn("segment_lower", F.lower("segment"))
    .withColumn("segment_lower_2", F.lower("segment"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    cleaned
    .filter(F.col("is_active"))
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.lead("credit_limit", 2).over(w2).alias("next_credit_limit"),
        F.first("age", ignorenulls=True).over(w3).alias("first_known_age"),
    )
)
