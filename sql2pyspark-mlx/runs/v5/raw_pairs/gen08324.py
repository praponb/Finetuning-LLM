from pyspark.sql import functions as F

base = df_shipments.filter(F.col("cost") > 50)

agg = (
    base
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
)

result = (
    agg
    .filter(F.col("cost_count") >= 7)
    .select("shipped_year", "cost_count", "unique_statuss")
)
