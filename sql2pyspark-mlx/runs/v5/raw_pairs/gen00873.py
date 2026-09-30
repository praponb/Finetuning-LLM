from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)

enriched = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w).alias("last3_avg_sqft"),
        F.count("sqft").over(w).alias("last3_count_sqft"),
        F.max("sqft").over(w).alias("last3_max_sqft"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "sqft",
        "last3_count_sqft",
        "last3_max_sqft",
        F.when(F.col("store_name").rlike("^(Downtown Express|Siam Flagship)$"), "CORE").when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "GROUP_A").otherwise("PRIORITY").alias("store_name_zone"),
    )
)
