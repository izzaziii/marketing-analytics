from typing import List, Dict, Optional, Union, Any
import pandas as pd
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    RunReportResponse,
    DateRange,
    Dimension,
    Metric,
    MetricType,
    DimensionExpression,
    FilterExpression,
    Filter,
    OrderBy,
)
from dotenv import load_dotenv
from client import GA4Client
import os

load_dotenv()

SESSION_PERFORMANCE_MARKETING_CHANNELS = os.getenv(
    "SESSION_PERFORMANCE_MARKETING_CHANNELS"
)


class GA4Reports(GA4Client):
    """Class for managing Google Analytics 4 reports that extends GA4Client."""

    def __init__(self, service_account_path: str, property_id: str):
        """Initialize GA4Reports with service account path and GA4 property ID."""
        super().__init__(service_account_path, property_id)
        # Dictionary to store predefined report configurations
        self.report_configs = {}
        # Initialize common report configurations
        self._initialize_report_configs()

    def _initialize_report_configs(self) -> None:
        """Initialize predefined report configurations."""
        # Daily Traffic report
        self.report_configs["daily_kpis"] = {
            "dimensions": ["date", SESSION_PERFORMANCE_MARKETING_CHANNELS],
            "metrics": ["totalUsers"],
        }

        # Page performance report
        self.report_configs["page_performance"] = {
            "dimensions": ["pageTitle", "pagePath", "fullPageUrl"],
            "metrics": [
                "screenPageViews",
                "averageSessionDuration",
                "bounceRate",
                "conversions",
            ],
        }

        # E-commerce report
        self.report_configs["ecommerce"] = {
            "dimensions": [
                "date",
                SESSION_PERFORMANCE_MARKETING_CHANNELS,
                "eventName",
                "customEvent:event_category",
                "customEvent:event_action",
                "customEvent:event_label",
            ],
            "metrics": ["totalUsers"],
        }

    def run_report(
        self,
        date_ranges: List[Dict[str, str]],
        dimensions: List[str],
        metrics: List[str],
        dimension_filter: Optional[FilterExpression] = None,
        metric_filter: Optional[FilterExpression] = None,
        limit: int = 10000,
        offset: int = 0,
        order_bys: Optional[List[OrderBy]] = None,
    ) -> RunReportResponse:
        """
        Run a custom report with the specified parameters.

        Args:
            date_ranges: List of date ranges [{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}]
            dimensions: List of dimension API names (e.g., ["sessionSource", "sessionMedium"])
            metrics: List of metric API names (e.g., ["sessions", "activeUsers"])
            dimension_filter: Optional dimension filter expression
            metric_filter: Optional metric filter expression
            limit: Maximum number of rows to return (default: 10000)
            offset: Offset for pagination (default: 0)
            order_bys: Optional list of OrderBy objects for sorting

        Returns:
            RunReportResponse object containing the report data
        """
        # Create dimension objects
        dimension_objs = [Dimension(name=d) for d in dimensions]

        # Create metric objects
        metric_objs = [Metric(name=m) for m in metrics]

        # Create date range objects
        date_range_objs = [
            DateRange(start_date=dr["start_date"], end_date=dr["end_date"])
            for dr in date_ranges
        ]

        # Build the report request
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dimension_objs,
            metrics=metric_objs,
            date_ranges=date_range_objs,
            limit=limit,
            offset=offset,
        )

        # Add dimension filter if provided
        if dimension_filter:
            request.dimension_filter = dimension_filter

        # Add metric filter if provided
        if metric_filter:
            request.metric_filter = metric_filter

        # Add order_bys if provided
        if order_bys:
            request.order_bys = order_bys

        # Execute the report request
        try:
            response = self.client.run_report(request)
            return response
        except Exception as e:
            raise Exception(f"Error running GA4 report: {e}")

    def get_predefined_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        additional_dimensions: Optional[List[str]] = None,
        additional_metrics: Optional[List[str]] = None,
        dimension_filter: Optional[FilterExpression] = None,
        metric_filter: Optional[FilterExpression] = None,
        limit: int = 10000,
    ) -> RunReportResponse:
        """
        Run a predefined report based on stored configurations with optional customizations.

        Args:
            report_type: Name of the predefined report (e.g., "user_acquisition")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            additional_dimensions: Optional list of additional dimensions to include
            additional_metrics: Optional list of additional metrics to include
            dimension_filter: Optional dimension filter expression
            metric_filter: Optional metric filter expression
            limit: Maximum number of rows to return

        Returns:
            RunReportResponse object containing the report data
        """
        if report_type not in self.report_configs:
            raise ValueError(
                f"Report type '{report_type}' not found in predefined configurations"
            )

        # Get base configuration
        config = self.report_configs[report_type]

        # Combine base dimensions with any additional dimensions
        dimensions = config["dimensions"].copy()
        if additional_dimensions:
            dimensions.extend([d for d in additional_dimensions if d not in dimensions])

        # Combine base metrics with any additional metrics
        metrics = config["metrics"].copy()
        if additional_metrics:
            metrics.extend([m for m in additional_metrics if m not in metrics])

        # Create date range
        date_ranges = [{"start_date": start_date, "end_date": end_date}]

        # Run the report
        return self.run_report(
            date_ranges=date_ranges,
            dimensions=dimensions,
            metrics=metrics,
            dimension_filter=dimension_filter,
            metric_filter=metric_filter,
            limit=limit,
        )

    def process_report_to_dataframe(self, response: RunReportResponse) -> pd.DataFrame:
        """
        Process a GA4 report response into a pandas DataFrame.

        Args:
            response: RunReportResponse object from the GA4 API

        Returns:
            pandas DataFrame containing the report data
        """
        # Create lists to store the data
        data = []

        # Extract dimension and metric headers
        dimension_headers = [header.name for header in response.dimension_headers]
        metric_headers = [header.name for header in response.metric_headers]
        all_headers = dimension_headers + metric_headers

        # Process each row in the response
        for row in response.rows:
            row_data = {}

            # Process dimensions
            for i, dimension in enumerate(row.dimension_values):
                row_data[dimension_headers[i]] = dimension.value

            # Process metrics
            for i, metric in enumerate(row.metric_values):
                row_data[metric_headers[i]] = metric.value

            data.append(row_data)

        # Create the DataFrame
        df = pd.DataFrame(data, columns=all_headers)

        # Convert numeric columns to appropriate types
        for metric in metric_headers:
            df[metric] = pd.to_numeric(df[metric])

        return df

    def create_filter_expression(
        self,
        field_name: str,
        operator: str,
        value: Any,
        is_dimension: bool = True,
        negate: bool = False,
    ) -> FilterExpression:
        """
        Create a filter expression for dimensions or metrics.

        Args:
            field_name: The dimension or metric name to filter on
            operator: The operator to use ('EXACT', 'CONTAINS', 'BEGINS_WITH', etc.)
            value: The value to compare against
            is_dimension: Whether this is a dimension filter (True) or metric filter (False)
            negate: Whether to negate this filter (creates a NOT filter)

        Returns:
            A FilterExpression object
        """
        field_filter = Filter()

        if is_dimension:
            field_filter.field_name = field_name

            if operator == "EXACT":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "EXACT"
            elif operator == "CONTAINS":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "CONTAINS"
            elif operator == "BEGINS_WITH":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "BEGINS_WITH"
            elif operator == "ENDS_WITH":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "ENDS_WITH"
            elif operator == "REGEXP":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "REGEXP"
            elif operator == "FULL_REGEXP":
                field_filter.string_filter.value = str(value)
                field_filter.string_filter.match_type = "FULL_REGEXP"
            # Add other operators as needed

            filter_expr = FilterExpression(filter=field_filter)
        else:
            field_filter.field_name = field_name

            if operator == "GREATER_THAN":
                field_filter.numeric_filter.operation = "GREATER_THAN"
                field_filter.numeric_filter.value.double_value = float(value)
            elif operator == "LESS_THAN":
                field_filter.numeric_filter.operation = "LESS_THAN"
                field_filter.numeric_filter.value.double_value = float(value)
            elif operator == "EQUAL":
                field_filter.numeric_filter.operation = "EQUAL"
                field_filter.numeric_filter.value.double_value = float(value)
            # Add other operators as needed

            filter_expr = FilterExpression(filter=field_filter)

        # Apply NOT if requested
        if negate:
            return FilterExpression(not_expression=filter_expr)
        else:
            return filter_expr

    def add_report_config(
        self, name: str, dimensions: List[str], metrics: List[str]
    ) -> None:
        """
        Add a custom report configuration to the reports dictionary.

        Args:
            name: Name of the report configuration
            dimensions: List of dimension API names
            metrics: List of metric API names
        """
        self.report_configs[name] = {"dimensions": dimensions, "metrics": metrics}

    def create_combined_filter(
        self, filters: List[FilterExpression], operator: str = "AND"
    ) -> FilterExpression:
        """
        Create a combined filter expression from multiple filters.

        Args:
            filters: List of FilterExpression objects to combine
            operator: Logical operator to use ('AND' or 'OR')

        Returns:
            A combined FilterExpression object
        """
        if not filters:
            raise ValueError("Must provide at least one filter")

        if len(filters) == 1:
            return filters[0]

        if operator == "AND":
            return FilterExpression(and_group={"expressions": filters})
        elif operator == "OR":
            return FilterExpression(or_group={"expressions": filters})
        else:
            raise ValueError(f"Unsupported operator: {operator}. Use 'AND' or 'OR'.")
