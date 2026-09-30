from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "store_name",
        "opened_date",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
    .orderBy(F.col("region_title").desc())
)
