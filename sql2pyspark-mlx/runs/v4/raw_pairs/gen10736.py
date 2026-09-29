from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") > 11)
    .select("id", "region", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("sqft") >= 4404)
        .select("id", "region", "opened_date")
    )
    .distinct()
)
