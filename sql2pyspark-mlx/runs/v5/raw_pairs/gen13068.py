from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "store_name",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)
