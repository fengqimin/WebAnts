from setuptools import setup, find_packages

VERSION = "0.1.1"
DESCRIPTION = "A simple asynchronous web crawler framework implemented using httpx"

setup(
    name="webants",
    version=VERSION,
    author="fengqimin",
    author_email="fengqimin@msn.com",
    python_requires=">=3.12",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=open("README.md", encoding="UTF8").read(),
    packages=find_packages(),
    keywords=["python", "async", "web crawler", "httpx"],
    install_requires=[
        "charset-normalizer>=3.4.1",
        "cssselect>=1.3.0",
        "httpx>=0.28.1",
        "lxml>=5.3.0",
        "multidict>=6.1.0",
        "pydantic>=2.11.3",
        "pyyaml>=6.0.2",
        "ujson>=5.10.0",
    ],
    license="MIT",
    url="https://github.com/fengqimin/WebAnts",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Microsoft :: Windows",
    ],
)
