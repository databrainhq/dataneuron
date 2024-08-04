import mimetypes
import os
import base64
import yaml


def clean_path(path):
    path = path.strip("'\"")
    path = path.replace("\\ ", " ")
    return os.path.normpath(path)


def format_yaml_for_prompt(yaml_data):
    return yaml.dump(yaml_data, default_flow_style=False)


def convert_to_base64(img_path):
    # image_path = clean_path(img_path)
    mime_type, _ = mimetypes.guess_type(img_path)
    with open(img_path, "rb") as image_file:
        image_data = base64.b64encode(image_file.read()).decode('utf-8')
        return mime_type, image_data
