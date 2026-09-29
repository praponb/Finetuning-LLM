from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city")
w2 = Window.orderBy("sqft", "id")
w_hist = Window.partitionBy("city").orderBy("opened_date", "id")
w4 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w2))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("running_sqft", F.sum("sqft").over(w4))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_share",
        "prev_sqft",
    )
)
