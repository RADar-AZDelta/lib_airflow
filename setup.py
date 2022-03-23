import setuptools

with open("./README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lib_airflow",
    version="0.0.1",
    author="RADar-AZDelta",
    author_email="innovatie@azdelta.be",
    description="Custom Airflow operators and hooks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RADar-AZDelta/lib_airflow",
    packages=setuptools.find_packages(),
    install_requires=[
        "lib_azdelta @ git+ssh://git@github.com/RADar-AZDelta/lib_azdelta@main",
        "apache-airflow-providers-http",
        "apache-airflow-providers-odbc",
        "apache-airflow-providers-google"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
