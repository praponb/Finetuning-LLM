from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.orderBy("region_avg_sqft", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .withColumn("city_max_sqft", F.max("sqft").over(w))
    .withColumn("region_avg_sqft", F.avg("sqft").over(w2))
    .withColumn("sqft_bucket", F.ntile(3).over(w3))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("city_max_sqft").over(w4).alias("city_max_sqft_avg_5"),
        F.first("sqft", ignorenulls=True).over(w5).alias("first_known_sqft"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "region",
        "opened_date",
        "city_max_sqft",
        "region_avg_sqft",
        "city_max_sqft_avg_5",
        "first_known_sqft",
        F.when(F.col("region").rlike("^(Central|South)$"), "EXTENDED").otherwise("PRIORITY").alias("region_zone"),
    )
    .orderBy("region", F.col("id").desc())
)
