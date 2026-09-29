from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date").between("2025-02-04", "2025-05-11")), "left_anti")
)

result = (
    not_flagged
    .select("id", "city", "region", "opened_date", "sqft")
    .orderBy(F.col("sqft").desc())
)
