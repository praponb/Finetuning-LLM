from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 4656) & (F.col("sqft") > 14314))
    .select(
        "id",
        "city",
        "store_name",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
    .orderBy(F.col("opened_date_plus_months").desc())
)
