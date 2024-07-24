import os
import click
from .claude_api import call_claude_api_with_pagination, call_claude_vision_api_with_pagination, stream_claude_response
from .openai_api import call_api_with_pagination, call_vision_api_with_pagination, stream_response
from ..utils.print import print_debug, print_info
from ..utils.stream_print import process_simplified_xml
import xml.etree.ElementTree as ET


def get_api_functions():
    llm_type = os.getenv('DATA_NEURON_LLM', 'claude').lower()
    if llm_type == 'claude':
        return call_claude_api_with_pagination, call_claude_vision_api_with_pagination, stream_claude_response
    elif llm_type in ['openai', 'azure', 'custom', 'ollama']:
        return call_api_with_pagination, call_vision_api_with_pagination, stream_response
    else:
        raise ValueError(f"Unsupported LLM type: {llm_type}")


def stream_neuron_api(query, include_context=False, instruction_prompt=None, print_chunk=False):
    _, _, stream_response = get_api_functions()

    xml_buffer = ""
    if print_chunk:
        print_info("DATA_NEURON: ")
        for chunk in stream_response(query, instruction_prompt):
            click.echo(chunk, nl=False)
        return None
    else:
        state = {
            'buffer': '',
            'in_step': False,
        }
        for chunk in stream_response(query, instruction_prompt):
            if print_chunk:
                click.echo(chunk, nl=False)
            else:
                process_simplified_xml(chunk, state)
            xml_buffer += chunk

        return xml_buffer


def call_neuron_api(query, include_context=False, instruction_prompt=None):
    call_api, _, _ = get_api_functions()
    response = call_api(query, include_context, instruction_prompt)
    return response
    # return parse_neuron_response(response)


def call_neuron_vision_api(query, image_path, include_context=False, instruction_prompt=None):
    _, call_vision_api, _ = get_api_functions()
    response = call_vision_api(
        query, image_path, include_context, instruction_prompt)
    return response
    # return parse_neuron_response(response)


def call_neuron_api_with_pagination(query, include_context=False, instruction_prompt=None):
    call_api, _, _ = get_api_functions()
    response = call_api(query, include_context, instruction_prompt)
    return response


def call_neuron_vision_api_with_pagination(query, image_path, include_context=False, instruction_prompt=None):
    _, call_vision_api, _ = get_api_functions()
    response = call_vision_api(
        query, image_path, include_context, instruction_prompt)
    return response
