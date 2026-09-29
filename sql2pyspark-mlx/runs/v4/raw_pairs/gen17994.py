from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city", "region")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-04").cast("date")) & F.upper("store_name").like("%LD %"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

result = (
    valid
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("city_sum_sqft", F.sum("sqft").over(w2))
)
