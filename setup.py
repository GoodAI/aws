import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aws",
    version="0.0.2",
    author="goodai",
    author_email="jaroslav.vitku@goodai.com",
    description="A small tool for managing AWS-machine-based experiments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/GoodAI/aws",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Ubuntu",
    ],
    package_data={"aws": ["words_alpha.txt", "aws_ignore.txt"]},
    include_package_data=True,
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
)
