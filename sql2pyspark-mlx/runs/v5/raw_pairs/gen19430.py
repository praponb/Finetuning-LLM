from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date_plus_53d").desc_nulls_first())
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("sale_date_plus_53d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("units").isNull())
    .withColumn("sale_date_plus_53d", F.date_add("sale_date", 53))
)

latest = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("units").over(w4).alias("running_units"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.region"), F.col("p2.opened_date"), F.col("p2.sqft"))
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_plus_53d",
        "row_seq",
        "running_units",
        "region",
        F.when(F.col("region").rlike("^(West|Central)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("region_zone"),
    )
    .orderBy(F.col("running_units").desc_nulls_first())
)
