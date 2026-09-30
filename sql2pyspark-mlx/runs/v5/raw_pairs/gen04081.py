from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("region").isin(["East", "Central", "South"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.avg("sqft").over(w2).alias("sqft_avg_5_prior"),
        F.ntile(10).over(w3).alias("sqft_bucket_2"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)
