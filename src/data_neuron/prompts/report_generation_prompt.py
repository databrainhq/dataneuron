def report_generation_prompt(dashboard_name, results, instruction):
    return f"""
    Generate a complete, self-contained HTML report for the dashboard '{dashboard_name}' based on the following instructions:
    {instruction}

    Dashboard metrics and their results:
    {results}

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
