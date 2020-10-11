import os

import setuptools

_dir_path = os.path.dirname(os.path.realpath(__file__))
_source_path = os.path.join(_dir_path, 'src/')

requirementPath = 'requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()

setuptools.setup(
    name="pm4pyspark",
    version=0.1,
    author="Niek Tax",
    author_email="niek.tax@booking.com",
    description="A python package that offers pyspark functionality for process mining "
                "that builds on top of python package pm4py",
    packages=setuptools.find_namespace_packages('src'),
    package_dir={'': 'src'},
    install_requires=install_requires,
    include_package_data=True,
    python_requires='>=3.5',
)
