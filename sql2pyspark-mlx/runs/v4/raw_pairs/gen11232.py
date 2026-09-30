from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

valid = (
    df_store_sales
    .filter(F.col("units") == 37)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.when(F.col("units") >= 43, "High").when(F.col("units") >= 8, "Medium").otherwise("Low").alias("units_tier"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
