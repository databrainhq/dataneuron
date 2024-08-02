import sys
import logging
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '..')))


def pytest_configure(config):
    logging.basicConfig(level=logging.DEBUG)
