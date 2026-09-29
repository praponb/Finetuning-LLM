from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("signup_date", "id")

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.concat("name", F.lit(" - "), "segment").alias("name_segment"),
        F.lower("email").alias("email_lower"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

result = (
    filtered
    .withColumn("city_max_age", F.max("age").over(w))
    .withColumn("next_age", F.lead("age", 2).over(w2))
)
