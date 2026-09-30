from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w_grp = Window.partitionBy("city").orderBy("sqft", "id")

with_windows = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.lag("sqft", 1).over(w_grp).alias("prev_sqft"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft_share",
        "prev_sqft",
        F.when(F.col("city").rlike("^(Bangkok|Berlin)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("id")
)
