from reports import GA4Reports
from dotenv import load_dotenv
import os

load_dotenv()

GOOGLE_ANALYTICS_PROPERTY = os.getenv("GOOGLE_ANALYTICS_PROPERTY")
SESSION_PERFORMANCE_MARKETING_CHANNELS = os.getenv(
    "SESSION_PERFORMANCE_MARKETING_CHANNELS"
)


def main():
    start_date = "2025-02-25"
    end_date = "2025-02-28"
    ga4 = GA4Reports(
        "/Users/izzaziskandar/Documents/GitHub Repos/marketing-analytics/ga4_credentials.json",
        GOOGLE_ANALYTICS_PROPERTY,
    )

    filter_pages = ga4.create_filter_expression(
        field_name="pagePath", operator="CONTAINS", value="/payment-method", negate=True
    )

    filter_events = ga4.create_filter_expression("eventName", "EXACT", "page_view")

    combined = ga4.create_combined_filter([filter_pages, filter_events], "AND")

    df = ga4.process_report_to_dataframe(
        ga4.get_predefined_report(
            "daily_kpis",
            start_date,
            end_date,
            [SESSION_PERFORMANCE_MARKETING_CHANNELS],
            dimension_filter=combined,
            limit=100000,
        )
    )

    print(df.head(30))


if __name__ == "__main__":
    main()
