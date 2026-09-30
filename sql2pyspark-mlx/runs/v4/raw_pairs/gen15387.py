from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("region").orderBy("sqft", "id")
w_part_2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date_month_end", "id")
w4 = Window.partitionBy("region").orderBy("store_name_length", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_stores
    .filter(F.length("store_name") > 12)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.length("store_name").alias("store_name_length"),
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    valid
    .select(
        "*",
        F.lead("sqft", 2).over(w_part).alias("next_sqft"),
        F.sum("sqft").over(w_part_2).alias("cumulative_sqft"),
        F.lag("store_name_length", 1).over(w3).alias("prev_store_name_length"),
        F.avg("store_name_length").over(w4).alias("store_name_length_avg_3"),
    )
)
