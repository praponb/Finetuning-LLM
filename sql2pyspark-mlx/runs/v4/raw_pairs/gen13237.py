from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

cleaned = (
    df_stores
    .filter(F.year("opened_date") >= 2025)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.col("sqft").cast("double").alias("sqft_num"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
