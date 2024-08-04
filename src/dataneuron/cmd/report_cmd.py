# src/dataneuron/cmd/report_cmd.py

import click
import os
import yaml
import re
from datetime import datetime
from ..core.dashboard_manager import DashboardManager
from ..db_operations.factory import DatabaseFactory
from ..api.main import call_neuron_vision_api, call_neuron_api
from ..utils.print import print_header, print_info, print_success, print_warning, styled_prompt


def generate_report():
    import pdfkit
    print_header("Generating Dashboard Report")

    dashboard_manager = DashboardManager()
    dashboards = dashboard_manager.list_dashboards()
    if not dashboards:
        print_warning("No dashboards found. Please create a dashboard first.")
        return

    print_info("Available dashboards:")
    for idx, dashboard in enumerate(dashboards, 1):
        click.echo(f"{idx}. {dashboard}")

    while True:
        choice = styled_prompt("Select a dashboard (number)")
        try:
            index = int(choice) - 1
            if 0 <= index < len(dashboards):
                dashboard_name = dashboards[index]
                break
        except ValueError:
            pass
        print_warning("Invalid selection. Please try again.")

    instruction = styled_prompt("Enter instructions for the report generation")
    image_path = styled_prompt(
        "Enter path to reference image (optional, press Enter to skip)")

    html_content = generate_report_html(
        dashboard_name, instruction, image_path.strip() or None, dashboard_manager)

    # Save HTML content to a file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = "reports"
    os.makedirs(report_dir, exist_ok=True)
    html_file = os.path.join(report_dir, f"report_{timestamp}.html")
    with open(html_file, 'w') as f:
        f.write(html_content)

    # Convert HTML to PDF
    pdf_file = os.path.join(report_dir, f"report_{timestamp}.pdf")
    pdfkit.from_file(html_file, pdf_file)

    print_success(f"Report generated successfully: {pdf_file}")


def generate_report_html(dashboard_name, instruction, image_path=None, dashboard_manager=None):
    if dashboard_manager is None:
        dashboard_manager = DashboardManager()

    dashboard = dashboard_manager.load_dashboard(dashboard_name)
    if not dashboard:
        return f"<html><body><h1>Error: Dashboard '{dashboard_name}' not found.</h1></body></html>"

    results = dashboard_manager.execute_dashboard_queries(dashboard_name)

    prompt = f"""
    Generate a complete, self-contained HTML report for the dashboard '{dashboard_name}' based on the following instructions:
    {instruction}

    Dashboard metrics and their results:
    {yaml.dump(results, default_flow_style=False)}

    Please create a visually appealing HTML page that includes:
    A title and brief description of the dashboard

    Important requirements for the HTML:
    1. The HTML should be a complete, valid document starting with <!DOCTYPE html>.
    2. Include all necessary CSS and JavaScript within the HTML file.
    3. If you need external libraries (e.g., for charts or styling), include the appropriate CDN links in the <head> section.
    4. Ensure all content, styling, and functionality are self-contained within this single HTML file.
    5. Use modern, responsive design principles to ensure the report looks good on various devices unless specified otherwise.
    6. No need of interactive elements or visualizations as it is generated for viewing.

    Your goal is to create a professional, visually appealing, and informative dashboard report that can be viewed as a standalone HTML file.
    """

    if image_path and os.path.exists(image_path):
        response = call_neuron_vision_api(prompt, image_path)
    else:
        response = call_neuron_api(prompt)

    return extract_html_from_response(response)


def extract_html_from_response(response):
    html_pattern = re.compile(
        r'<!DOCTYPE html>.*?</html>', re.DOTALL | re.IGNORECASE)
    match = html_pattern.search(response)

    if match:
        html_content = match.group(0)
    else:
        html_content = response

    return html_content


if __name__ == '__main__':
    generate_report()
