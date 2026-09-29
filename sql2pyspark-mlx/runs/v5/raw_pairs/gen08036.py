from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w_running = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy("region", "city").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.lead("sqft", 2).over(w2).alias("next_sqft"),
        F.percent_rank().over(w_running).alias("sqft_percent_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
