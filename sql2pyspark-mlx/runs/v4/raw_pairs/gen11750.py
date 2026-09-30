from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("revenue_tier").orderBy(F.col("corr_revenue_units").desc())

matched_keys = (
    df_store_sales
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units").between(34, 36))
        .select("store_id", "id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("revenue") >= 3500, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        F.when(F.col("units") >= 31, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .filter(F.col("corr_revenue_units") > 0)
    .withColumn("corr_revenue_units_rank", F.rank().over(w))
)
