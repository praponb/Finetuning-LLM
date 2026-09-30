from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

cleaned = (
    df_suppliers
    .filter(~F.col("city").isin(["London", "Berlin", "New York"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat_ws("/", "city", "country").alias("city_country_key"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "clearance")), "left_anti")
)

result = (
    kept
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.6, "LARGE").otherwise("SMALL"))
    .cube("rating_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping_id().alias("grouping_level"),
    )
)
