from setuptools import setup, find_packages

setup(
    name="parqeutdb",  
    version="0.1.0",  
    author="Logan Lang", 
    author_email="lllang@mix.wvu.edu", 
    description="ParquetDB is a lightweight database-like system built on top of Apache Parquet files using PyArrow.",  
    long_description=open("README.md").read(), 
    long_description_content_type="text/markdown",
    # url="https://github.com/yourusername/your_package_name",  # Replace with your package's URL
    packages=find_packages(),
    install_requires=[
        # Add your package dependencies here, e.g.:
        'pyarrow',
        'pandas',
        'numpy',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Replace with your license
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",  # Specify the minimum Python version required
)