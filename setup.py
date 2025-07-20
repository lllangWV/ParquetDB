from setuptools import setup
from setuptools_scm import ScmVersion


def version_for_project(version: ScmVersion) -> str:
   return str(version.tag)
   
setup()
