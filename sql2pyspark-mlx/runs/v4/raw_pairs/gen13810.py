from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-20")))
    .select(
        "*",
        F.initcap(F.trim("segment")).alias("segment_title"),
        F.lower("email").alias("email_lower"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
