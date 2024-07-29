import re
from queue import Queue
import click


def process_simplified_xml(chunk, state):
    state['buffer'] += chunk

    # Process SQL tag
    sql_match = re.search(r'<\s*sql\s*>(.*?)<\s*/\s*sql\s*>',
                          state['buffer'], re.DOTALL | re.IGNORECASE)
    if sql_match:
        sql_content = sql_match.group(1).strip()
        click.echo(click.style("\nGenerated SQL Query:", fg="blue", bold=True))
        click.echo(sql_content)
        state['buffer'] = state['buffer'][sql_match.end():]
        if sql_content and not state['query_executed']:
            state['sql_query'] = sql_content
            state['sql_queue'].put(sql_content)
            state['query_executed'] = True

    # Process explanation tag
    explanation_match = re.search(
        r'<\s*explanation\s*>(.*?)<\s*/\s*explanation\s*>', state['buffer'], re.DOTALL | re.IGNORECASE)
    if explanation_match:
        explanation_content = explanation_match.group(1).strip()
        click.echo(click.style("\nExplanation:", fg="green", bold=True))
        click.echo(explanation_content)
        state['buffer'] = state['buffer'][explanation_match.end():]

    # Process references tag
    references_match = re.search(
        r'<\s*references\s*>(.*?)<\s*/\s*references\s*>', state['buffer'], re.DOTALL | re.IGNORECASE)
    if references_match:
        references_content = references_match.group(1).strip()
        click.echo(click.style("\nReferences:", fg="yellow", bold=True))
        click.echo(references_content)
        state['buffer'] = state['buffer'][references_match.end():]


def stream_and_print_simplified_xml(chunks):
    state = {'buffer': '', 'sql_queue': Queue()}
    for chunk in chunks:
        process_simplified_xml(chunk, state)

    if state['buffer'].strip():
        click.echo(f"\nRemaining Content: {state['buffer'].strip()}")

    click.echo()  # Final newline
