from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(8561, 14699))
    .select(
        "id",
        "opened_date",
        F.concat("region", F.lit(" "), "store_name").alias("region_store_name"),
    )
)
