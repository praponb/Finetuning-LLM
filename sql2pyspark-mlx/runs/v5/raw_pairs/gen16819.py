from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "sqft")
)

result = (
    ranked
    .select(
        "id",
        "city",
        "sqft",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc())
)
