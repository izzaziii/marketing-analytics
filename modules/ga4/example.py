from pathlib import Path
import pandas as pd
import os
from dotenv import load_dotenv

# Assuming the GA4Reports class is defined in a file named ga4_client.py
from reports import GA4Reports

load_dotenv()

GOOGLE_ANALYTICS_PROPERTY = os.getenv("GOOGLE_ANALYTICS_PROPERTY")


def main():
    # Initialize the GA4Reports client
    ga4_reports = GA4Reports(
        service_account_path="/Users/izzaziskandar/Documents/GitHub Repos/marketing-analytics/ga4_credentials.json",
        property_id=GOOGLE_ANALYTICS_PROPERTY,
    )

    # Example 1: Run a predefined user acquisition report
    print("Running user acquisition report...")
    report_response = ga4_reports.get_predefined_report(
        report_type="user_acquisition", start_date="2025-02-25", end_date="2025-02-28"
    )

    # Convert the response to a DataFrame
    df_acquisition = ga4_reports.process_report_to_dataframe(report_response)
    print(f"User acquisition report shape: {df_acquisition.shape}")
    print(df_acquisition.head())

    # Example 2: Create and run a custom report
    print("\nRunning custom report...")
    custom_response = ga4_reports.run_report(
        date_ranges=[{"start_date": "2025-02-25", "end_date": "2025-02-28"}],
        dimensions=["deviceCategory", "operatingSystem", "browser"],
        metrics=["sessions", "screenPageViews", "conversions"],
    )

    df_custom = ga4_reports.process_report_to_dataframe(custom_response)
    print(f"Custom report shape: {df_custom.shape}")
    print(df_custom.head())

    # Example 3: Add a new report configuration and use it
    ga4_reports.add_report_config(
        name="device_report",
        dimensions=["deviceCategory", "operatingSystem", "browser"],
        metrics=["sessions", "screenPageViews", "conversions", "engagementRate"],
    )

    # Use the new report configuration
    device_response = ga4_reports.get_predefined_report(
        report_type="device_report",
        start_date="2025-02-25",
        end_date="2025-02-28",
        additional_metrics=["bounceRate"],  # Add an extra metric
    )

    df_device = ga4_reports.process_report_to_dataframe(device_response)
    print(f"\nDevice report shape: {df_device.shape}")
    print(df_device.head())

    # Example 4: Using filters
    print("\nRunning filtered report...")
    # Create a dimension filter for organic traffic
    organic_filter = ga4_reports.create_filter_expression(
        field_name="sessionMedium", operator="EXACT", value="organic", is_dimension=True
    )

    filtered_response = ga4_reports.get_predefined_report(
        report_type="user_acquisition",
        start_date="2025-02-25",
        end_date="2025-02-28",
        dimension_filter=organic_filter,
    )

    df_filtered = ga4_reports.process_report_to_dataframe(filtered_response)
    print(f"Filtered report shape: {df_filtered.shape}")
    print(df_filtered.head())


if __name__ == "__main__":
    main()
