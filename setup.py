from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "Async web crawler based on asyncio and aiohttp."

setup(
    name="webants",
    version=VERSION,
    author="fengqimin",
    author_email="fengqimin@msn.com",
    python_requires=">=3.10",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=open("README.md", encoding="UTF8").read(),
    packages=find_packages(),
    keywords=["python", "async", "web crawler"],
    install_requires=[
        "lxml>=4.9.3",
        "aiohttp~=3.8.5",
        "multidict~=6.0.4",
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
