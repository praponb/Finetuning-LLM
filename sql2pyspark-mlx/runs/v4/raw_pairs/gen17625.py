from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-05-03"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.date_add("opened_date", 64).alias("opened_date_plus_64d"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
