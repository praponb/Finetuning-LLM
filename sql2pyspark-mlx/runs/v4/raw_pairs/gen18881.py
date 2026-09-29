from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("country_length").desc(), "id")
w2 = Window.orderBy(F.col("country_length").desc_nulls_first())
w3 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.length("country").alias("country_length"))
)

kept = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isin([29, 40])), "left_anti")
)

metrics = (
    kept
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("country_length_rank", F.rank().over(w2))
    .withColumn("city_max_rating", F.max("rating").over(w3))
)

valid = (
    metrics
    .filter(F.col("country").isin(["SG", "JP", "DE"]))
    .select(
        "*",
        (F.col("rating") - F.col("city_max_rating")).alias("rating_minus_city_max_rating"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum("rating_minus_city_max_rating").alias("total_rating_minus_city_max_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("jp_events") < 1)
    .orderBy(F.col("row_seq_tier").desc_nulls_first())
)
