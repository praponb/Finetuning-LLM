from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.orderBy(F.col("sqft").desc(), "id")
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region") == "North")
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .withColumn("sqft_avg_2", F.avg("sqft").over(w))
    .withColumn("sqft_bucket", F.ntile(4).over(w_ordered))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w_ordered))
)
