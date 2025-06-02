from setuptools import setup, find_packages

setup(
    name='db-utils',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A utility package for database operations in Redshift',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',  # PostgreSQL adapter for Python
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)