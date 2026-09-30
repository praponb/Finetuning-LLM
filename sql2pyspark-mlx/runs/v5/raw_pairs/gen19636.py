from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "South", "West"]) & (F.col("region").isin(["Central", "South"]) | F.col("city").isin(["Chiang Mai", "Bangkok", "London"])))
    .select(
        "store_name",
        "region",
        "city",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("opened_date_plus_months").desc())
)
