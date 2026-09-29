from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city")

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("Lio%"))
    .withColumn("country_lower", F.lower("country"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") >= 850), "left_anti")
)

with_windows = (
    eligible
    .withColumn("running_rating", F.sum("rating").over(w2))
    .withColumn("running_rating_2", F.sum("rating").over(w_running))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w4)), 4))
)

by_type = (
    with_windows
    .groupBy("city")
    .pivot("country", ["JP", "UK", "TH"])
    .agg(F.max("rating_share"))
)

result = (
    by_type
    .select(
        "city",
        "UK",
        F.when(F.col("city").rlike("^(Singapore|New York)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_first())
)
