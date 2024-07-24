import os
from .claude_api import call_claude_api_with_pagination, call_claude_vision_api_with_pagination, stream_claude_response
from .openai_api import call_api_with_pagination, call_vision_api_with_pagination, stream_response
import xml.etree.ElementTree as ET


def get_api_functions():
    llm_type = os.getenv('DATA_NEURON_LLM', 'claude').lower()
    if llm_type == 'claude':
        return call_claude_api_with_pagination, call_claude_vision_api_with_pagination, stream_claude_response
    elif llm_type in ['openai', 'azure', 'custom', 'ollama']:
        return call_api_with_pagination, call_vision_api_with_pagination, stream_response
    else:
        raise ValueError(f"Unsupported LLM type: {llm_type}")


def stream_neuron_api(query, chat_history=None, instruction_prompt=None):
    _, _, stream_response = get_api_functions()
    return stream_response(query, chat_history, instruction_prompt)


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
