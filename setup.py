from setuptools import setup


setup(
    name='dbt-mermaid',
    version='0.1.0',
    py_modules=['cli'],
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'erd = cli:erd',
        ],
    },
)