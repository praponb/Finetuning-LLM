from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "cost",
        "price",
        "launch_date",
        F.array_join("tags", "|").alias("tags_text"),
    )
)
