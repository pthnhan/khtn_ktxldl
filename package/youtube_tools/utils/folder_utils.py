import os


def create_folder(folder):
    try:
        if not os.path.exists(folder):
            os.makedirs(folder)
    except:
        pass


# get parent folder
def get_parent_folder(filepath):
    return os.path.abspath(os.path.join(filepath, os.pardir))


# get children folder
def get_children_folder(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]