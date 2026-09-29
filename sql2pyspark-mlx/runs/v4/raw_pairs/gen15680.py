from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date_day", "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-10-20"))) & F.col("region").isNotNull())
    .withColumn("opened_date_day", F.dayofmonth("opened_date"))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

result = (
    filtered
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w3))
)
