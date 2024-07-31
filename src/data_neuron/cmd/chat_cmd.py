import click
from ..core import DataNeuron
from ..utils.print import print_info, print_success, print_prompt


def chat_cmd(context):
    """Start an interactive chat session with the database."""
    dn = DataNeuron(db_config='database.yaml', context=context)
    dn.initialize()

    print_info("Starting chat session. Type 'exit' to end the conversation.")

    while True:
        user_input = click.prompt("You", prompt_suffix="> ")

        if user_input.lower() in ['exit', 'quit', 'bye']:
            print_info("Ending chat session. Goodbye!")
            break

        response = dn.chat(user_input)

        print_success("\nAssistant:")
        print_info(response)
        print()  # Add a blank line for better readability
