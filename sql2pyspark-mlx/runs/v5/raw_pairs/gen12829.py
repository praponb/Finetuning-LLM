from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")

filtered = (
    df_customers
    .filter((F.year("signup_date") == 2023) & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.initcap(F.trim("segment")).alias("segment_title"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "credit_limit", "segment_title", "resolved_city")
)
