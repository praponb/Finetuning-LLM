from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region", "city").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-30").cast("date")) & F.col("region").isNull())
    .withColumn("sqft_whole", F.floor("sqft"))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

result = (
    valid
    .filter(F.col("region").isNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        F.rank().over(w_grp).alias("sqft_rank"),
        F.sum("sqft_whole").over(w2).alias("cumulative_sqft_whole"),
        F.last("sqft", ignorenulls=True).over(w3).alias("last_known_sqft"),
        F.last("region", ignorenulls=True).over(w4).alias("last_known_region"),
    )
)
