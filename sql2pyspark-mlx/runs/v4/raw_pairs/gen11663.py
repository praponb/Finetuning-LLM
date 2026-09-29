from pyspark.sql import functions as F

min_opened_date = df_accounts.agg(F.min("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") == min_opened_date)
    .select("id", "opened_date")
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(1)
)
