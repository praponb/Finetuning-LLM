from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("least_revenue_units", "id")
w2 = Window.orderBy(F.col("revenue_whole").desc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy("revenue_whole_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7, 8, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.ceil("revenue").alias("revenue_whole"),
        F.least("revenue", "units").alias("least_revenue_units"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.region"), F.col("d.store_name"), F.col("d.sqft"))
)

scored = (
    joined
    .select(
        "*",
        F.lead("revenue_whole", 1).over(w).alias("next_revenue_whole"),
        F.cume_dist().over(w2).alias("revenue_whole_cume_dist"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.last("revenue_whole_cume_dist", ignorenulls=True).over(w3).alias("last_known_revenue_whole_cume_dist"),
        F.lead("revenue_whole", 1).over(w4).alias("next_revenue_whole_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("units") >= 41, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("last_known_revenue_whole_cume_dist"), 2).alias("avg_last_known_revenue_whole_cume_dist"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.round(F.sum("revenue_whole") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("revenue_whole_per_south"),
        F.sum("revenue_whole_cume_dist").alias("total_revenue_whole_cume_dist"),
    )
    .orderBy(F.col("quarter_start").desc())
)
