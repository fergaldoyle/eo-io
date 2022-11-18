import yaml


def read_yaml(file_name):
    with open(file_name) as f:
        return yaml.load(f.read(), Loader=yaml.FullLoader)
