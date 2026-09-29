from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-12-13").cast("date")) & F.col("channel").isNotNull())
    .withColumn("promo_code_lower", F.lower("promo_code"))
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
