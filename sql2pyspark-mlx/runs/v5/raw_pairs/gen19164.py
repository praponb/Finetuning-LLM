from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at_period").desc_nulls_first(), "id")

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Edge"))))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.date_trunc("YEAR", "started_at").alias("started_at_period"),
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("country", F.lit("OTHER")).alias("country_filled"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country_title",
        "country_filled",
    )
)
