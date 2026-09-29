from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "North", "West"]))
    .select(
        "id",
        "region",
        "city",
        "opened_date",
        F.substring("region", 1, 2).alias("region_prefix"),
    )
    .orderBy("opened_date")
)
