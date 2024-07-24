import click
from colorama import Fore, Style
import shutil


def print_error(message):
    click.echo(f"{Fore.RED}✘ {message}{Style.RESET_ALL}")


def print_prompt(message, indent=0):
    click.echo(f"{' ' * indent}{Fore.MAGENTA}{message}{Style.RESET_ALL}")


def print_success(message):
    click.echo(f"{Fore.GREEN}✔ {message}{Style.RESET_ALL}")


def print_info(message, indent=0):
    click.echo(f"{' ' * indent}{Fore.BLUE} {message}{Style.RESET_ALL}")


def print_warning(message):
    click.echo(f"{Fore.YELLOW}⚠ {message}{Style.RESET_ALL}")


def print_debug(message):
    click.echo(click.style(f"DEBUG: {message}", fg="cyan"))


def print_step(step_number, total_steps, message):
    click.echo(
        f"{Fore.CYAN}[{step_number}/{total_steps}] {message}{Style.RESET_ALL}")


def create_confirmation_box(command, action):
    terminal_width = shutil.get_terminal_size().columns
    # Max width of 60 or terminal width - 4
    box_width = min(terminal_width - 4, 60)

    # Center the title
    title = "Confirmation"
    title_line = f"| {title.center(box_width - 2)} |"

    command_line = f"| {command.center(box_width - 2)} |"
    action_line = f"| {action.center(box_width - 2)} |"

    confirmation_box = f"""
{Fore.CYAN}{' ' * ((terminal_width - box_width) // 2)}┌{'─' * box_width}┐
{' ' * ((terminal_width - box_width) // 2)}{title_line}
{' ' * ((terminal_width - box_width) // 2)}|{' ' * box_width}|
{' ' * ((terminal_width - box_width) // 2)}{command_line}
{' ' * ((terminal_width - box_width) // 2)}{action_line}
{' ' * ((terminal_width - box_width) // 2)}└{'─' * box_width}┘{Style.RESET_ALL}
"""

    return confirmation_box


def print_header(message):
    click.echo(f"\n🎾 {Fore.CYAN}{message}{Style.RESET_ALL}\n")
