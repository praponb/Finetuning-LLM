from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("sqft").asc_nulls_last())
w3 = Window.partitionBy("city", "region").orderBy(F.col("store_sale_revenue").desc_nulls_first())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-18").cast("date")) & (F.upper(F.trim("store_name")) == "OLD TOWN"))
    .withColumn("store_name_upper", F.upper("store_name"))
    .withColumn("city_lower", F.lower("city"))
)

base = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-08-23").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_upper",
        "city_lower",
        F.when(F.col("city").isin(["Chiang Mai", "Tokyo"]), "Tier 1").otherwise("Other").alias("city_group"),
        F.when(F.col("city") == "London", 1).otherwise(0).alias("is_london"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.sqft",
        "s.city_group",
        F.col("c.sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel").isin(["web", "partner"])), "left_anti")
)

matched_keys = (
    kept
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.col("sqft").between(6874, 8523))
        .select("city", "region")
    )
)

verified = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "store_name",
        "city",
        "sqft",
        "city_group",
        "store_sale_revenue",
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.rank().over(w3).alias("store_sale_revenue_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("store_sale_revenue") < 2500)
    .select(
        "store_name",
        "sqft",
        "city_group",
        "store_sale_revenue",
        F.col("store_sale_revenue").cast("int").alias("store_sale_revenue_int"),
    )
)

result = valid.select("sqft", "city_group", "store_sale_revenue_int")
