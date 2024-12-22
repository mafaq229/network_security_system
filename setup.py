'''
The setup.py file is an essential part of packaging and 
distributing Python projects. It is used by setuptools 
(or distutils in older Python versions) to define the configuration 
of your project, such as its metadata, dependencies, and more
'''

from setuptools import find_packages, setup
from typing import List

def get_requirements() -> List[str]:
    """
        This function reads the requirements.txt file and returns a list of required packages
    """
    requirement_list: List[str] = []
    try:
        with open('requirements.txt') as file:
            # read lines from the file
            lines = file.readlines()
            # process each line
            for line in lines:
                requirement = line.strip()
                # ignore empty lines and "-e."
                if requirement and requirement != "-e.":
                    requirement_list.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found")
    
    return requirement_list


setup(
    name="NetworkSecuritySystem",
    version="0.0.1",
    author="Muhammad Afaq",
    author_email="muhammadafaq1999@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)
