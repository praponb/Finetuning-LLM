from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_customers
    .withColumn("country_count_age", F.count("age").over(w))
    .withColumn("age_percent_rank", F.percent_rank().over(w2))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w3))
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w4))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "age",
        "country_count_age",
        "first_known_segment",
    )
    .orderBy(F.col("age").desc())
)
