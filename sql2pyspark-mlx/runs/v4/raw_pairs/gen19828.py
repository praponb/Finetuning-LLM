from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy(F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"))
    .pivot("brand", ["Globex", "Acme"])
    .agg(F.max("price"))
)
