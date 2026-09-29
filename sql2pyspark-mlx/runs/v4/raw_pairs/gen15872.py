from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("region", "city").orderBy(F.col("city_length").desc(), "id")

filtered = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .select("*", F.length("city").alias("city_length"))
)

result = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("sqft_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
