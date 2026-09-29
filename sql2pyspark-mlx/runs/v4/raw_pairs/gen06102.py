from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

latest = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Fashion"), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNull())
    .select("*", F.length("country").alias("country_length"))
)

latest_suppliers = (
    valid
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

by_type = (
    latest_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Berlin"])
    .agg(F.avg("rating").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "country",
        F.col("Chiang Mai_value_sum").alias("chiang_mai_value_sum"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)

result = (
    by_type
    .select(
        "country",
        "chiang_mai_value_sum",
        "chiang_mai_item_count",
        "berlin_value_sum",
        "berlin_item_count",
    )
    .orderBy(F.col("country").desc_nulls_last())
    .limit(20)
)
