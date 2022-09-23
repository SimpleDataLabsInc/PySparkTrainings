from setuptools import setup, find_packages
setup(
    name = 'ExploreTCPH',
    version = '1.0',
    packages = find_packages(include = ('exploretcph*', )),
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.4'],
    entry_points = {
'console_scripts' : [
'main = exploretcph.pipeline:main', ], },
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
