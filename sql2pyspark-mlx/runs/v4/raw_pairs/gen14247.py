from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("opened_date_month_end", "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.when(F.col("city").isin(["Singapore", "New York"]), "Core").otherwise("Other").alias("city_group"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.lead("sqft", 1).over(w2).alias("next_sqft"),
    )
)

valid = (
    enriched
    .filter((F.col("opened_date") >= F.lit("2024-03-12").cast("date")) & F.col("region").isNotNull() & (F.col("next_sqft") >= 4917))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "city_group",
        "region_filled",
        "sqft_share",
        "next_sqft",
        (F.col("next_sqft") + F.col("sqft")).alias("next_sqft_plus_sqft"),
        F.add_months("opened_date_month_end", 3).alias("opened_date_month_end_plus_months"),
    )
)

result = (
    valid
    .select("id", "city", "sqft_share", "next_sqft_plus_sqft")
    .unpivot(["id", "city"], ["sqft_share", "next_sqft_plus_sqft"], "kpi", "reading")
    .filter(F.col("reading").isNotNull())
)
