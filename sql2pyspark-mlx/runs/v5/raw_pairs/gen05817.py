from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("cost").desc())
w3 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("cost_tier").orderBy(F.col("umbrella_events").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.round((F.col("rating") * 0.85), 2).alias("rating_adjusted"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("rating_adjusted").isNull())
    .withColumn("is_th", F.when(F.col("country") == "TH", 1).otherwise(0))
)

valid_suppliers = (
    valid
    .filter(F.col("rating_adjusted") <= 56)
    .select("id", "supplier_name", F.length("country").alias("country_length"))
)

latest_match = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", F.col("c.brand"), F.col("c.cost"))
)

metrics = (
    latest_match
    .withColumn("cost_dense_rank", F.dense_rank().over(w2))
    .withColumn("running_cost", F.sum("cost").over(w3))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cost") >= 770, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "brand",
    )
    .agg(
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.sort_array(F.collect_set("brand")).alias("brand_list_2"),
    )
    .withColumn("umbrella_events_dense_rank", F.dense_rank().over(w4))
)
