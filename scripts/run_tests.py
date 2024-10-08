import unittest
from parquetdb import config



# Discover all the tests in the 'tests' folder
loader = unittest.TestLoader()
suite = loader.discover(config.tests_dir)

# Run the test suite
runner = unittest.TextTestRunner()
runner.run(suite)