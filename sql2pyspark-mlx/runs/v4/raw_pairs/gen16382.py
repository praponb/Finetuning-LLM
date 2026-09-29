from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("supplier_name_length", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("city").orderBy("supplier_name_length", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("rating", "id")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy("supplier_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("rating").between(2.4, 3.2))
    .withColumn("supplier_name_length", F.length("supplier_name"))
    .withColumn("city_group", F.when(F.col("city").isin(["London", "Bangkok"]), "Priority").otherwise("Other"))
)

filtered = (
    valid
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "supplier_name_length",
        "city_group",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

enriched = (
    filtered
    .withColumn("supplier_name_length_avg_5_prior", F.avg("supplier_name_length").over(w_hist))
    .withColumn("supplier_name_length_avg_3_prior", F.avg("supplier_name_length").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w4)), 4))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w5))
)

daily = (
    enriched
    .cube("city")
    .agg(
        F.percentile_approx("rating_share", 0.5).alias("rating_share_p50"),
        F.count("*").alias("cnt"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.percentile_approx("supplier_name_length_avg_5_prior", 0.5).alias("supplier_name_length_avg_5_prior_p50"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("rating_stddev") >= 0)
)

result = (
    daily
    .select(
        "city",
        "rating_share_p50",
        "cnt",
        "rating_stddev",
        "supplier_name_length_avg_5_prior_p50",
        "grouping_level",
    )
)
