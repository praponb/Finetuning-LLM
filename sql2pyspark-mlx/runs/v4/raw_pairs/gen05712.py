from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select("id", "opened_date", "region")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") < "2025-08-01")
        .select("id", "opened_date", "region")
    )
    .orderBy("region")
)
