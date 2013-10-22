
from setuptools import setup, find_packages


setup(
    name="hot-redis",
    version="0.1.0",
    author="Stephen McDonald",
    author_email="steve@jupo.org",
    description="Higher Order Types for Redis",
    long_description=open("README.rst").read(),
    url="http://github.com/stephenmcd/hot-redis",
    packages=find_packages(),
    install_requires=["sphinx-me", "redis"],
    zip_safe=False,
    include_package_data=True,
    test_suite="hot_redis.tests",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Object Brokering",
    ]
)
