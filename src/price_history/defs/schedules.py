import dagster as dg

daily_schedule = dg.ScheduleDefinition(
    name="daily_price_scrape",
    cron_schedule="0 3 * * *",
    execution_timezone="America/Argentina/Buenos_Aires",
    target=dg.AssetSelection.all(),
)

defs = dg.Definitions(schedules=[daily_schedule])
