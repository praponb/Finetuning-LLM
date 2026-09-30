from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")

deduped = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "region", "sqft")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("sqft") >= 16941, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
    .filter(F.col("sqft_bit_and") < 1000)
    .orderBy(F.col("sqft_bit_and").desc_nulls_first())
)
