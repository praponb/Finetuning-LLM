from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating_filled").asc_nulls_last(), "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    deduped
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") > "2024-11-01"), "left_anti")
)

summary = (
    eligible
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
    )
)

result = (
    summary
    .select("city", "berlin_rows", "country_list", "distinct_ids")
    .orderBy("distinct_ids", F.col("city").desc())
)
