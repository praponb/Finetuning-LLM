from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("city").alias("city_length"),
        F.substring("city", 1, 4).alias("city_prefix"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

latest_match_suppliers = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

eligible = (
    latest_match_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") < F.col("s.launch_date")), "left_anti")
)

daily = (
    eligible
    .groupBy(
        F.when(F.col("rating") >= 3.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.avg("product_cost"), 2).alias("avg_product_cost"),
    )
)

result = daily.select("rating_tier", "country_list", "chiang_mai_rating", "avg_product_cost")
