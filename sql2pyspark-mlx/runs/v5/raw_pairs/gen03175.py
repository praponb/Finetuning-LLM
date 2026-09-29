from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        F.count("sqft").over(w_ordered).alias("last7_count_sqft"),
        F.avg("sqft").over(w_ordered).alias("last7_avg_sqft"),
        F.sum("sqft").over(w_ordered).alias("last7_sum_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "opened_date",
        "sqft",
        "last7_count_sqft",
        "last7_avg_sqft",
        "last7_sum_sqft",
        "sqft_avg_6",
    )
    .orderBy(F.col("last7_sum_sqft").desc_nulls_first(), "sqft_avg_6")
)
