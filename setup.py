from setuptools import setup, find_packages

#setup(name="gap_filling", packages=find_packages())

setup(
    name="gap_filling",
    version="0.1.0",
    description="This project takes Climate Trace data, compares it to other existing datasources, and fills gaps as "
    "needed.",
    author="Megan Baker and Alexandra Stephens",
    author_email="megan.baker@jhuapl.edu, alexandra.stephens@jhuapl.edu",
    packages=find_packages(exclude=("tests", "docs")),
    include_package_data=True,
    package_data={
        "gap_filling": ["data/*.csv"]
    }
)
