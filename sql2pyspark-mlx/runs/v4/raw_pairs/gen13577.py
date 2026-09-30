from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(F.length("supplier_name") >= 6)
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
