from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft_scaled").desc(), "id")
w2 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("sqft_scaled", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-05-02").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") * 1.15), 2).alias("sqft_scaled"),
        F.concat_ws(",", "store_name", "city").alias("store_name_city_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

result = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.first("sqft", ignorenulls=True).over(w2).alias("first_known_sqft"),
        F.last("region", ignorenulls=True).over(w3).alias("last_known_region"),
        F.avg("sqft_scaled").over(w_recent).alias("sqft_scaled_avg_6"),
    )
)
