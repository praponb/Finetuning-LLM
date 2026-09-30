from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("store_name").like("Air%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.units"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

valid = (
    latest_match
    .filter(F.col("store_sale_revenue") >= 3100)
    .withColumn("sqft_scaled", F.round((F.col("sqft") * 1.15), 2))
)

result = (
    valid
    .groupBy("region")
    .pivot("city", ["Singapore", "New York", "Chiang Mai"])
    .agg(F.max("sqft").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "region",
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_item_count").alias("new_york_item_count"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
    )
)
