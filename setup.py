import os
from pathlib import Path

from setuptools import find_packages, setup

# from subprocess import check_call
# from setuptools.command.develop import develop
# from setuptools.command.install import install
# from setuptools.command.sdist import sdist

with open("./README.md", "r") as fh:
    _long_description = fh.read()

#_here = Path(__file__).parent.absolute()

# def gitcmd_update_submodules():
# 	'''	Check if the package is being deployed as a git repository. If so, recursively
# 		update all dependencies.

# 		@returns True if the package is a git repository and the modules were updated.
# 			False otherwise.
# 	'''
# 	if os.path.exists(os.path.join(_here, '.git')):
# 		check_call(['git', 'submodule', 'update', '--init', '--recursive'])
# 		return True

# 	return False


# class gitcmd_develop(develop):
# 	'''	Specialized packaging class that runs git submodule update --init --recursive
# 		as part of the update/install procedure.
# 	'''
# 	def run(self):
# 		gitcmd_update_submodules()
# 		develop.run(self)


# class gitcmd_install(install):
# 	'''	Specialized packaging class that runs git submodule update --init --recursive
# 		as part of the update/install procedure.
# 	'''
# 	def run(self):
# 		gitcmd_update_submodules()
# 		install.run(self)


# class gitcmd_sdist(sdist):
# 	'''	Specialized packaging class that runs git submodule update --init --recursive
# 		as part of the update/install procedure;.
# 	'''
# 	def run(self):
# 		gitcmd_update_submodules()
# 		sdist.run(self)


setup(
    # cmdclass={
	# 	'develop': gitcmd_develop, 
	# 	'install': gitcmd_install, 
	# 	'sdist': gitcmd_sdist,
	# },
    name="lib_airflow",
    version="0.0.1",
    author="RADar-AZDelta",
    author_email="innovatie@azdelta.be",
    description="Custom Airflow operators and hooks",
    long_description=_long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/RADar-AZDelta/lib_airflow",
    packages=find_packages(exclude=['docs', 'tests', 'examples']),
    install_requires=[
        "lib_azdelta @ git+ssh://git@github.com/RADar-AZDelta/lib_azdelta@main",
		#f'lib_azdelta @ file://localhost/{_here}/libs/lib_azdelta',
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
