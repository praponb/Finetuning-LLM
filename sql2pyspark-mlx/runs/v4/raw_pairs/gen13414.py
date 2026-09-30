from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy(F.col("rating").desc())
w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

enriched = (
    filtered
    .withColumn("rating_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w_running))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
    .withColumn("country_count_rating", F.count("rating").over(w4))
)

valid = (
    enriched
    .filter(F.col("city") == "Singapore")
    .select(
        "*",
        F.coalesce("first_known_rating", F.lit(0)).alias("first_known_rating_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

result = (
    valid
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_title",
        "rating_percent_rank",
        "first_known_rating",
        "country_count_rating",
        F.when(F.col("supplier_name").rlike("^(Siam Parts Co|EuroGoods GmbH)$"), "CORE").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").desc_nulls_first(), F.col("id").desc())
    .limit(5)
)
