from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")

enriched = (
    df_products
    .select(
        "*",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.percentile_approx("running_avg_cost", 0.5).alias("running_avg_cost_p50"),
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("category") == "Home", F.col("running_min_cost"))).alias("home_avg_running_min_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("cost_per_globex"),
    )
)
