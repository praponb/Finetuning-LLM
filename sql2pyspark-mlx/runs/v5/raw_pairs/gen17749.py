from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")

enriched = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("units").over(w).alias("prev_units"),
        F.lead("units").over(w).alias("next_units"),
        F.row_number().over(w).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"), "product_id")
    .agg(
        F.min("row_seq").alias("min_row_seq"),
        F.percentile_approx("next_units", 0.9).alias("next_units_p90"),
        F.round(F.avg("next_revenue"), 2).alias("avg_next_revenue"),
        F.percentile_approx("next_units", 0.5).alias("next_units_p50"),
    )
    .filter(F.col("next_units_p50") < 1000000)
    .orderBy(F.col("next_units_p50").desc_nulls_last())
)
