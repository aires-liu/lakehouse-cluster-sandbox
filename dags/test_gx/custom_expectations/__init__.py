import os
import importlib

current_dir = os.path.dirname(__file__)
for filename in os.listdir(current_dir):
    if filename.startswith("expect_") and filename.endswith(".py") and filename != "__init__.py":
        module_name = os.path.splitext(filename)[0]
        module = importlib.import_module(f"test_gx.custom_expectations.{module_name}")
        for attr in dir(module):
            if attr.startswith("Expect"):
                globals()[attr] = getattr(module, attr)