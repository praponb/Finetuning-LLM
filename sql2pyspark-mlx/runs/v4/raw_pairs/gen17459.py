from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city")

enriched = (
    df_stores
    .select(
        "*",
        F.lead("sqft", 2, 0).over(w).alias("next_sqft"),
        F.stddev_samp("sqft").over(w2).alias("city_stddev_samp_sqft"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "next_sqft",
        "city_stddev_samp_sqft",
        F.when(F.col("store_name").rlike("^(Old Town|Siam Flagship)$"), "EXTENDED").otherwise("CORE").alias("store_name_zone"),
    )
)
